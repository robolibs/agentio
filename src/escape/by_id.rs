use std::fmt;
use peerbus::{EndpointId, Node, PipClient, PutClient, QueClient, ReqClient, Subscriber};

use crate::error::Result;
use crate::naming::normalize_topic;

/// Escape hatch for invoking peerbus operations directly against a specific `EndpointId`,
/// bypassing topic resolution within the Agent directory.
#[derive(Clone)]
pub struct ById<'a> {
    node: &'a Node,
    peer_id: EndpointId,
}

impl<'a> fmt::Debug for ById<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ById")
            .field("peer_id", &self.peer_id)
            .finish()
    }
}

impl<'a> ById<'a> {
    pub fn new(node: &'a Node, peer_id: EndpointId) -> Self {
        Self { node, peer_id }
    }

    /// Return the target `EndpointId`.
    pub fn endpoint_id(&self) -> EndpointId {
        self.peer_id
    }

    /// Subscribe to `topic` hosted by the target `EndpointId` directly.
    pub fn subscribe<T>(&self, topic: &str) -> Result<Subscriber<T>>
    where
        T: datapod::DataPod + 'static,
        <T as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        self.node.subscriber::<T>(self.peer_id, &normalized).map_err(Into::into)
    }

    /// Open a req/res client targeting `topic` hosted by the target `EndpointId` directly.
    pub fn req_client<Req, Res>(&self, topic: &str) -> Result<ReqClient<Req, Res>>
    where
        Req: datapod::DataPod + 'static,
        <Req as datapod::DataPod>::Header: datapod::LeWireHeader,
        Res: datapod::DataPod + 'static,
        <Res as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        self.node.req_client::<Req, Res>(self.peer_id, &normalized).map_err(Into::into)
    }

    /// Open a queue client targeting `topic` on the target `EndpointId` directly.
    pub fn que_client<Que, Ans>(&self, topic: &str) -> Result<QueClient<Que, Ans>>
    where
        Que: datapod::DataPod + 'static,
        <Que as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ans: datapod::DataPod + 'static,
        <Ans as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        self.node.que_client::<Que, Ans>(self.peer_id, &normalized).map_err(Into::into)
    }

    /// Open a put/ack client targeting `topic` on the target `EndpointId` directly.
    pub fn put_client<Put, Ack>(&self, topic: &str) -> Result<PutClient<Put, Ack>>
    where
        Put: datapod::DataPod + 'static,
        <Put as datapod::DataPod>::Header: datapod::LeWireHeader,
        Ack: datapod::DataPod + 'static,
        <Ack as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        self.node.put_client::<Put, Ack>(self.peer_id, &normalized).map_err(Into::into)
    }

    /// Open a pip client targeting `topic` on the target `EndpointId` directly.
    pub fn pip_client<ClientMsg, ServerMsg>(&self, topic: &str) -> Result<PipClient<ClientMsg, ServerMsg>>
    where
        ClientMsg: datapod::DataPod + 'static,
        <ClientMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
        ServerMsg: datapod::DataPod + 'static,
        <ServerMsg as datapod::DataPod>::Header: datapod::LeWireHeader,
    {
        let normalized = normalize_topic(topic)?;
        self.node.pip_client::<ClientMsg, ServerMsg>(self.peer_id, &normalized).map_err(Into::into)
    }
}
