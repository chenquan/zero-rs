#![feature(type_alias_impl_trait)]

use std::convert::Infallible;
use std::future::Future;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, atomic, Mutex, RwLock};
use std::{net, process, thread, time};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::error::Error;
use std::f32::consts::E;
use std::hash::Hash;
use std::ops::{Add, Index};
use std::sync::atomic::Ordering;
use async_broadcast::{Receiver, Sender, SendError};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use etcd_client::{Client, EventType, GetOptions, KeyValue, ResponseHeader, WatchOptions};
use etcd_client::TxnOpResponse::Get;
use tracing_subscriber::fmt::writer::EitherWriter::B;
use volo::context::Endpoint;
use volo::discovery::{Change, Discover, Instance};
use volo::net::Address;
use volo::Unwrap;
use url::{Url, ParseError};
use std::borrow::BorrowMut;

#[derive(Clone)]
pub struct EtcdDiscover {
    instances: Arc<DashMap<String, Arc<Instance>>>,
    client: Client,
    service_name: String,
}


impl EtcdDiscover {
    pub fn new(service_name: String, client: Client) -> Self {
        Self { client, instances: Arc::new(DashMap::new()), service_name }
    }
}

impl Discover for EtcdDiscover {
    type Key = String;
    type Error = Infallible;
    type DiscFut<'a> = impl Future<Output=Result<Vec<Arc<Instance>>, Self::Error>> + 'a;

    fn discover(&self, e: &Endpoint) -> Self::DiscFut<'_> {
        let service_name = self.service_name.clone();
        let mut client = self.client.clone();
        let instances_arc = Arc::clone(&self.instances);
        async move {
            match client.get(service_name.as_str(), Some(GetOptions::default().with_prefix())).await {
                Ok(it) => {
                    let instance = to_vec_instance(it.kvs());
                    for x in it.kvs() {
                        if let Ok(k) = x.key_str() {
                            instances_arc.insert(k.to_string(), Arc::new(Instance {
                                address: Address::Ip(x.value_str().unwrap().parse().unwrap()),
                                weight: 1,
                                tags: Default::default(),
                            }));
                        }
                    }

                    tracing::info!("发现节点个数{}",instances_arc.len());

                    Ok(instance)
                }
                Err(e) => {
                    Ok(vec![])
                }
            }
        }
    }

    fn key(&self, endpoint: &Endpoint) -> Self::Key {
        self.service_name.clone().to_string()
    }

    fn watch(&self, _keys: Option<&[Self::Key]>) -> Option<Receiver<Change<Self::Key>>> {
        let (sender, receiver) = async_broadcast::broadcast(2);
        let service_name = self.service_name.clone();
        let mut client = self.client.clone();
        let instances_arc = Arc::clone(&self.instances);

        tokio::spawn(async move {
            let mut revision = 0;
            match client.get(service_name.as_str(), Some(GetOptions::default().with_prefix())).await {
                Ok(it) => {
                    if let Some(h) = it.header() {
                        revision = h.revision()
                    }

                    let instance = to_vec_instance(it.kvs());
                    if let Err(e) = sender.broadcast(Change {
                        key: service_name.to_string(),
                        all: instance,
                        added: vec![],
                        updated: vec![],
                        removed: vec![],
                    }).await {
                        tracing::error!("发送数据错误:{}",e);
                    }
                    tracing::info!("监听节点个数{}",instances_arc.len());
                }
                Err(e) => {}
            }

            let option = Some(WatchOptions::default().with_prefix().with_start_revision(revision + 1));
            let (_, mut stream) = client.watch(service_name.as_str(), option).await.unwrap();
            while let Some(resp) = stream.message().await.unwrap() {
                let mut added: Vec<Arc<Instance>> = vec![];
                let mut updated: Vec<Arc<Instance>> = vec![];
                let mut removed: Vec<Arc<Instance>> = vec![];


                for x in resp.events().clone() {
                    match x.event_type() {
                        EventType::Delete => {
                            if let Some(it) = x.kv() {
                                if let Ok(k) = it.key_str() {
                                    tracing::info!("移除节点 => {}",k);
                                    let value_opt = instances_arc.get(k);
                                    if value_opt.is_none() {
                                        continue;
                                    }

                                    let instance = value_opt.unwrap();
                                    removed.push(instance.clone());
                                    tracing::info!("移除节点成功 => {}",instance.address);
                                    instances_arc.remove(k);
                                }
                            }
                        }
                        EventType::Put => {
                            if let Some(it) = x.kv() {
                                if let Ok(k) = it.key_str() {
                                    match instances_arc.entry(k.to_string()) {
                                        Entry::Occupied(e) => {
                                            // exit
                                            tracing::info!("有更新节点 => {}",k);

                                            if let Some(instance) = to_instance(it) {
                                                e.replace_entry(instance.clone());

                                                tracing::info!("更新节点成功 => {}",instance.address);
                                                updated.push(instance.clone());
                                            }
                                        }
                                        Entry::Vacant(e) => {
                                            tracing::info!("有新增节点 => {}",k);


                                            if let Some(instance) = to_instance(it) {
                                                e.insert(instance.clone());
                                                tracing::info!("新增节点成功 => {}",instance.address);
                                                added.push(instance.clone())
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                tracing::info!("节点个数{}",instances_arc.len());
                instances_arc.iter().for_each(|it| tracing::info!("监听==> {} 地址：{}",it.key(),it.value().address));

                // 广播节点变更
                if let Err(e) = sender.broadcast(Change {
                    key: service_name.to_string(),
                    all: instances_arc.clone().iter().map(|it| it.value().clone()).collect(),
                    added,
                    updated,
                    removed,
                }).await {
                    tracing::error!("发送数据错误:{}",e);
                }
            }
        });

        tracing::info!("开启监听注册中心");
        Some(receiver)
    }
}


fn to_vec_instance(kvs: &[KeyValue]) -> Vec<Arc<Instance>> {
    kvs.iter().filter_map(|kv| to_instance(kv)).collect()
}

fn to_instance(x: &KeyValue) -> Option<Arc<Instance>> {
    let x = x.value_str().ok()?;
    let addr = x.parse().ok()?;
    Some(Arc::new(
        Instance {
            address: Address::Ip(addr),
            weight: 1,
            tags: Default::default(),
        }
    ))
}


