use std::future::Future;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use volo::context::Endpoint;
use volo::discovery::{Change, Discover, Instance};
use volo::layer::LayerExt;
use volo::loadbalance::error::LoadBalanceError;
use volo::loadbalance::LoadBalance;
use volo::net::Address;
use num_integer::Roots;
use volo::new_type;

const INIT_SUCCESS: i64 = 1000;
const THROTTLE_SUCCESS: i64 = INIT_SUCCESS / 2;
const PENALTY: i64 = i64::MAX;
const FORCE_PICK: i64 = time::Duration::SECOND.as_millis() as i64;


pub struct P2c<K>
    where K: Hash + PartialEq + Eq + Send + Sync + 'static

{
    router: DashMap<K, Vec<Arc<SubConn>>>,

}

impl<D> LoadBalance<D> for P2c<D::Key>
    where D: Discover, {
    type InstanceIter = InstancePicker;

    type GetFut<'future> =
    impl Future<Output=Result<Self::InstanceIter, LoadBalanceError>> + Send + 'future
        where
            Self: 'future;

    fn get_picker<'future>(&'future self, endpoint: &'future Endpoint, discover: &'future D) -> Self::GetFut<'future> where Self: 'future {
        async {
            let key = discover.key(endpoint);
            let _ = match self.router.entry(key) {
                Entry::Occupied(e) => {
                    // e.get().clone()
                }
                Entry::Vacant(e) => {}
            };
            //
            // let x: Vec<Arc<Instance>> = discover
            //     .discover(endpoint)
            //     .await
            //     .map_err(|err| err.into())?;

            Ok(InstancePicker { conns: Arc::new(vec![]) })
        }
    }

    fn rebalance(&self, changes: Change<D::Key>) {
        // match self.router.entry(changes.key) {
        //     Entry::Occupied(e) => {
        //         // changes.removed
        //
        //         e.replace_entry(Arc::new(InstancePicker {}));
        //     }
        //     Entry::Vacant(e) => {}
        // }
    }
}

pub struct InstancePicker {
    // 可用的连接
    conns: Arc<Vec<SubConn>>,
}

impl InstancePicker {
    fn new(conns: Arc<Vec<SubConn>>) -> Self {
        Self { conns }
    }
}


struct SubConn {
    lag: AtomicU64,
    inflight: AtomicI64,
    success: AtomicU64,
    requests: AtomicI64,
    last: AtomicI64,
    pick: AtomicI64,
    instance: Instance,
}

impl SubConn {
    fn healthy(&self) -> bool {
        self.success.load(Ordering::SeqCst) > THROTTLE_SUCCESS as u64
    }

    fn load(&self) -> i64 {
        let lag: i64 = (self.lag.load(Ordering::SeqCst) + 1).sqrt() as i64;
        let load = lag * (self.inflight.load(Ordering::SeqCst) + 1);
        if load == 0 {
            return PENALTY;
        }

        load
    }
}


impl Iterator for InstancePicker {
    type Item = Address;

    fn next(&mut self) -> Option<Self::Item> {
        let conns = &self.conns;
        match conns.len() {
            0 => {
                None
            }
            1 => {
                Some(conns[0].instance.address.clone())
            }
            2 => {
                None
            }
            _ => {
                None
            }
        }
    }
}

impl InstancePicker {
    fn choose(&self, c1: SubConn, c2: Option<SubConn>) -> SubConn {
        let now = chrono::Utc::now().timestamp();
        if c2.is_none() {
            c1.last.store(now, Ordering::SeqCst);
            return c1;
        }
        let c2 = c2.unwrap();

        let mut c1 = c1;
        let mut c2 = c2;

        if c1.load() > c2.load() {
            let t = c1;
            c1 = c2;
            c2 = t;
        }

        let pick = c2.pick.load(Ordering::SeqCst);
        if now - pick > FORCE_PICK
            && c2.pick.compare_and_swap(pick, now, Ordering::SeqCst) == pick {
            return c2;
        }

        c1.last.store(now, Ordering::SeqCst);
        c1
    }
}