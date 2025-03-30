use std::{iter::repeat, sync::Arc};

use iroh::NodeId;
use polestar::id::IdMap;
use tokio::sync::Mutex;

use crate::basic::Connections;

#[derive(Debug)]
pub struct Event<N, C> {
    node: N,
    event: EventType<N, C>,
}

impl<N, C> std::fmt::Display for Event<N, C>
where
    N: std::fmt::Display,
    C: std::fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let node = &self.node;
        match &self.event {
            EventType::OpenConnection { to, conn } => {
                write!(f, "{:>16} : {node} ~> {to} : {conn}", "OpenConnection")
            }
            EventType::AcceptConnection { from, conn } => {
                write!(f, "{:>16} : {node} <~ {from} : {conn}", "AcceptConnection")
            }
            EventType::CloseConnection { to, conn } => {
                write!(f, "{:>16} : {node} ~~ {to} : {conn}", "CloseConnection")
            }
            EventType::OpenStream { to, conn } => {
                write!(f, "{:>16} : {node} ~> {to} : {conn}", "OpenStream")
            }
            EventType::AcceptStream { from, conn } => {
                write!(f, "{:>16} : {node} <~ {from} : {conn}", "AcceptStream")
            }
            EventType::EndStream { to, conn } => {
                write!(f, "{:>16} : {node} ~~ {to} : {conn}", "EndStream")
            }
        }
    }
}
impl<N, C> Event<N, C> {
    pub fn new(node: impl Into<N>, event: EventType<impl Into<N>, C>) -> Self {
        let node = node.into();
        let event = event.map_nodes(Into::into);
        Self { node, event }
    }
}

#[derive(Debug)]
pub enum EventType<N, C> {
    OpenConnection { to: N, conn: C },
    AcceptConnection { from: N, conn: C },
    CloseConnection { to: N, conn: C },
    OpenStream { to: N, conn: C },
    AcceptStream { from: N, conn: C },
    EndStream { to: N, conn: C },
}

impl<N, C> EventType<N, C> {
    pub fn map_nodes<M>(self, mut f: impl FnMut(N) -> M) -> EventType<M, C> {
        match self {
            EventType::OpenConnection { to, conn } => EventType::OpenConnection { to: f(to), conn },
            EventType::AcceptConnection { from, conn } => EventType::AcceptConnection {
                from: f(from),
                conn,
            },
            EventType::CloseConnection { to, conn } => {
                EventType::CloseConnection { to: f(to), conn }
            }
            EventType::OpenStream { to, conn } => EventType::OpenStream { to: f(to), conn },
            EventType::AcceptStream { from, conn } => EventType::AcceptStream {
                from: f(from),
                conn,
            },
            EventType::EndStream { to, conn } => EventType::EndStream { to: f(to), conn },
        }
    }

    pub fn map_conns<D>(self, mut f: impl FnMut(C) -> D) -> EventType<N, D> {
        match self {
            EventType::OpenConnection { to, conn } => {
                EventType::OpenConnection { to, conn: f(conn) }
            }
            EventType::AcceptConnection { from, conn } => EventType::AcceptConnection {
                from,
                conn: f(conn),
            },
            EventType::CloseConnection { to, conn } => {
                EventType::CloseConnection { to, conn: f(conn) }
            }
            EventType::OpenStream { to, conn } => EventType::OpenStream { to, conn: f(conn) },
            EventType::AcceptStream { from, conn } => EventType::AcceptStream {
                from,
                conn: f(conn),
            },
            EventType::EndStream { to, conn } => EventType::EndStream { to, conn: f(conn) },
        }
    }

    pub fn remote(&self) -> &N {
        match self {
            EventType::OpenConnection { to, .. } => to,
            EventType::AcceptConnection { from, .. } => from,
            EventType::CloseConnection { to, .. } => to,
            EventType::OpenStream { to, .. } => to,
            EventType::AcceptStream { from, .. } => from,
            EventType::EndStream { to, .. } => to,
        }
    }
}

#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    derive_more::Display,
    derive_more::Debug,
    derive_more::Deref,
    derive_more::From,
)]
#[display("n{_0}")]
#[debug("n{_0}")]
pub struct Nid(usize);

impl polestar::id::Id for Nid {}

#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    derive_more::Display,
    derive_more::Debug,
    derive_more::Deref,
    derive_more::From,
)]
#[display("c{_0}")]
#[debug("c{_0}")]
pub struct ConId(usize);

impl polestar::id::Id for ConId {}

pub type EventMappingShared = Option<Arc<Mutex<EventMapping>>>;

#[derive(Default)]
pub struct EventMapping {
    pub nodes: IdMap<NodeId, Nid>,
    pub conns: IdMap<u64, ConId>,
}

impl EventMapping {
    pub fn apply(&mut self, event: Event<NodeId, u64>) -> Event<Nid, ConId> {
        assert_ne!(event.node, *event.event.remote(), "{event:?}");
        Event {
            node: self.nodes.lookup(event.node).unwrap(),
            event: event
                .event
                .map_nodes(|node| self.nodes.lookup(node).unwrap())
                .map_conns(|conn| self.conns.lookup(conn).unwrap()),
        }
    }

    pub(crate) fn print_info(&mut self, node_id: NodeId, conns: &Connections) {
        let node_id = self.nodes.lookup(node_id).unwrap();

        let initiated = conns
            .initiated
            .iter()
            .map(|((k, _), _)| self.nodes.lookup(*k).unwrap())
            .collect::<Vec<_>>();

        let accepted = conns
            .accepted
            .inner
            .iter()
            .flat_map(|((k, _), v)| repeat(self.nodes.lookup(*k).unwrap()).take(v.len()))
            .collect::<Vec<_>>();

        println!(
            "CONNECTIONS {node_id} : initiated={:?}, accepted={:?}",
            initiated, accepted
        );
    }
}

pub fn emit_event(
    event: Event<NodeId, u64>,
    node_id: NodeId,
    lock: &mut EventMapping,
    conns: Option<&Connections>,
) {
    let event = lock.apply(event);
    println!("{}", event);
    if let Some(conns) = conns {
        println!();
        lock.print_info(node_id, conns);
        println!();
    }
}
