use std::future::Future;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;
use dashmap::DashMap;
use volo::context::Endpoint;
use volo::discovery::{Change, Discover};
use volo::loadbalance::error::LoadBalanceError;
use volo::loadbalance::LoadBalance;
use volo::loadbalance::random::InstancePicker;

pub struct P2c<K>
    where K: Hash + PartialEq + Eq + Send + Sync + 'static

{
    router: DashMap<K, Arc<String>>,

}

impl<D> LoadBalance<D> for P2c<D::Key>
    where D: Discover, {
    type InstanceIter = InstancePicker;

    type GetFut<'future> =
    impl Future<Output=Result<Self::InstanceIter, LoadBalanceError>> + Send + 'future
        where
            Self: 'future;

    fn get_picker<'future>(&'future self, endpoint: &'future Endpoint, discover: &'future D) -> Self::GetFut<'future> where Self: 'future {
        todo!()
    }

    fn rebalance(&self, changes: Change<D::Key>) {
        todo!()
    }
}

struct SubConn {}