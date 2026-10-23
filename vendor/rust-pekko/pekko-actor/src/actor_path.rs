//! Actor addressing and path resolution

use serde::{Deserialize, Serialize};
use std::fmt;

/// Address of an actor system (local or remote)
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorAddress {
    pub protocol: String,
    pub system: String,
    pub host: Option<String>,
    pub port: Option<u16>,
}

impl ActorAddress {
    /// Create a local actor address
    pub fn local(system: &str) -> Self {
        Self {
            protocol: "pekko".to_string(),
            system: system.to_string(),
            host: None,
            port: None,
        }
    }

    /// Create a remote actor address
    pub fn remote(system: &str, host: &str, port: u16) -> Self {
        Self {
            protocol: "pekko".to_string(),
            system: system.to_string(),
            host: Some(host.to_string()),
            port: Some(port),
        }
    }

    /// Check if this is a local address
    pub fn is_local(&self) -> bool {
        self.host.is_none() && self.port.is_none()
    }
}

impl fmt::Display for ActorAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let (Some(host), Some(port)) = (&self.host, self.port) {
            write!(f, "{}://{}@{}:{}", self.protocol, self.system, host, port)
        } else {
            write!(f, "{}://{}", self.protocol, self.system)
        }
    }
}

/// Hierarchical path to an actor
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ActorPath {
    pub address: ActorAddress,
    pub elements: Vec<String>,
}

impl ActorPath {
    /// Create a new actor path
    pub fn new(address: ActorAddress, elements: Vec<String>) -> Self {
        Self { address, elements }
    }

    /// Create a root path (no elements)
    pub fn root(address: ActorAddress) -> Self {
        Self {
            address,
            elements: vec![],
        }
    }

    /// Create a child path by appending an element
    pub fn child(&self, name: &str) -> Self {
        let mut elements = self.elements.clone();
        elements.push(name.to_string());
        Self {
            address: self.address.clone(),
            elements,
        }
    }

    /// Get the parent path (if any)
    pub fn parent(&self) -> Option<Self> {
        if self.elements.is_empty() {
            None
        } else {
            let mut elements = self.elements.clone();
            elements.pop();
            Some(Self {
                address: self.address.clone(),
                elements,
            })
        }
    }

    /// Get the name of this actor (last element)
    pub fn name(&self) -> Option<&str> {
        self.elements.last().map(|s| s.as_str())
    }

    /// Get the depth of this path
    pub fn depth(&self) -> usize {
        self.elements.len()
    }

    /// Check if this is a child of another path
    pub fn is_child_of(&self, other: &ActorPath) -> bool {
        self.address == other.address
            && self.elements.len() == other.elements.len() + 1
            && self.elements[..other.elements.len()] == other.elements
    }

    /// Check if this is a descendant of another path
    pub fn is_descendant_of(&self, other: &ActorPath) -> bool {
        self.address == other.address
            && self.elements.len() > other.elements.len()
            && self.elements[..other.elements.len()] == other.elements
    }

    /// Parse an actor path from a string
    pub fn parse(path_str: &str) -> Result<Self, PathParseError> {
        // Example: "pekko://system@host:port/user/actor1/actor2"
        let parts: Vec<&str> = path_str.split("://").collect();
        if parts.len() != 2 {
            return Err(PathParseError::InvalidFormat(
                "Missing protocol separator".to_string(),
            ));
        }

        let protocol = parts[0].to_string();
        let remainder = parts[1];

        let (system_part, path_part) = if let Some(slash_pos) = remainder.find('/') {
            (&remainder[..slash_pos], &remainder[slash_pos + 1..])
        } else {
            (remainder, "")
        };

        let address = if system_part.contains('@') {
            // Remote address: system@host:port
            let parts: Vec<&str> = system_part.split('@').collect();
            if parts.len() != 2 {
                return Err(PathParseError::InvalidFormat(
                    "Invalid remote address format".to_string(),
                ));
            }
            let system = parts[0].to_string();
            let host_port = parts[1];
            
            let host_port_parts: Vec<&str> = host_port.split(':').collect();
            if host_port_parts.len() != 2 {
                return Err(PathParseError::InvalidFormat(
                    "Invalid host:port format".to_string(),
                ));
            }
            
            let host = host_port_parts[0].to_string();
            let port = host_port_parts[1].parse::<u16>()
                .map_err(|_| PathParseError::InvalidFormat("Invalid port number".to_string()))?;
            
            ActorAddress {
                protocol,
                system,
                host: Some(host),
                port: Some(port),
            }
        } else {
            // Local address: system
            ActorAddress {
                protocol,
                system: system_part.to_string(),
                host: None,
                port: None,
            }
        };

        let elements = if path_part.is_empty() {
            vec![]
        } else {
            path_part.split('/').map(|s| s.to_string()).collect()
        };

        Ok(ActorPath { address, elements })
    }
}

impl fmt::Display for ActorPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.address)?;
        for element in &self.elements {
            write!(f, "/{}", element)?;
        }
        Ok(())
    }
}

/// Error type for actor path parsing
#[derive(Debug, thiserror::Error)]
pub enum PathParseError {
    #[error("Invalid path format: {0}")]
    InvalidFormat(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_address() {
        let addr = ActorAddress::local("test-system");
        assert_eq!(addr.system, "test-system");
        assert!(addr.is_local());
        assert_eq!(addr.to_string(), "pekko://test-system");
    }

    #[test]
    fn test_remote_address() {
        let addr = ActorAddress::remote("test-system", "localhost", 2552);
        assert_eq!(addr.system, "test-system");
        assert!(!addr.is_local());
        assert_eq!(addr.to_string(), "pekko://test-system@localhost:2552");
    }

    #[test]
    fn test_actor_path_creation() {
        let addr = ActorAddress::local("test-system");
        let path = ActorPath::new(addr, vec!["user".to_string(), "test-actor".to_string()]);
        
        assert_eq!(path.name(), Some("test-actor"));
        assert_eq!(path.depth(), 2);
        assert_eq!(path.to_string(), "pekko://test-system/user/test-actor");
    }

    #[test]
    fn test_path_hierarchy() {
        let addr = ActorAddress::local("test-system");
        let parent = ActorPath::new(addr.clone(), vec!["user".to_string()]);
        let child = parent.child("test-actor");
        
        assert!(child.is_child_of(&parent));
        assert!(child.is_descendant_of(&parent));
        assert_eq!(child.parent(), Some(parent));
    }

    #[test]
    fn test_path_parsing() {
        let path_str = "pekko://test-system/user/test-actor";
        let path = ActorPath::parse(path_str).unwrap();
        
        assert_eq!(path.address.system, "test-system");
        assert!(path.address.is_local());
        assert_eq!(path.elements, vec!["user", "test-actor"]);
        assert_eq!(path.to_string(), path_str);
    }

    #[test]
    fn test_remote_path_parsing() {
        let path_str = "pekko://test-system@localhost:2552/user/test-actor";
        let path = ActorPath::parse(path_str).unwrap();
        
        assert_eq!(path.address.system, "test-system");
        assert!(!path.address.is_local());
        assert_eq!(path.address.host, Some("localhost".to_string()));
        assert_eq!(path.address.port, Some(2552));
        assert_eq!(path.elements, vec!["user", "test-actor"]);
    }
}