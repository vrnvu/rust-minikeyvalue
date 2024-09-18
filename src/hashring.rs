use hashring::HashRing;

/// `hashring`: HashRing to use for storing the data.
/// `replicas`: The number of replicas to create for the data. Default is 3.
/// `subvolumes`: The number of subvolumes, i.e., disks per machine. Default is 10.
pub fn get_volume(
    key: &str,
    hashring: &HashRing<String>,
    replicas: usize,
    subvolumes: u32,
) -> Vec<String> {
    let volumes = hashring.get_with_replicas(&key, replicas).unwrap();

    if volumes.len() == 1 {
        return volumes;
    }

    volumes
        .into_iter()
        .map(|volume| {
            let volume_md5 = md5::compute(&volume);
            let subvolume_hash = (u32::from(volume_md5[12]) << 24)
                + (u32::from(volume_md5[13]) << 16)
                + (u32::from(volume_md5[14]) << 8)
                + u32::from(volume_md5[15]);
            format!("{}/sv{:02X}", volume, subvolume_hash % subvolumes)
        })
        .collect::<Vec<String>>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_ring() {
        let mut ring: HashRing<String> = HashRing::new();

        ring.batch_add(vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "wow".to_string(),
        ]);

        assert_eq!(ring.get(&"1").unwrap(), "foo");
        assert_eq!(ring.get(&"2").unwrap(), "foo");
        assert_eq!(ring.get(&"3").unwrap(), "baz");
        assert_eq!(ring.get(&"4").unwrap(), "bar");
    }

    #[test]
    fn test_get_with_replicas() {
        let mut ring: HashRing<String> = HashRing::new();

        ring.batch_add(vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "wow".to_string(),
        ]);

        let nodes = ring.get_with_replicas(&1, 3).unwrap();
        assert_eq!(nodes[0], "foo");
        assert_eq!(nodes[1], "wow");
        assert_eq!(nodes[2], "bar");

        let nodes = ring.get_with_replicas(&2, 3).unwrap();
        assert_eq!(nodes[0], "baz");
        assert_eq!(nodes[1], "foo");
        assert_eq!(nodes[2], "wow");

        let nodes = ring.get_with_replicas(&3, 3).unwrap();
        assert_eq!(nodes[0], "wow");
        assert_eq!(nodes[1], "bar");
        assert_eq!(nodes[2], "baz");
    }

    #[test]
    fn test_get_volume() {
        let mut ring: HashRing<String> = HashRing::new();

        ring.batch_add(vec![
            "foo".to_string(),
            "bar".to_string(),
            "baz".to_string(),
            "wow".to_string(),
        ]);

        let key = "1";
        let volumes = get_volume(key, &ring, 3, 10);
        assert_eq!(volumes[0], "foo/sv00");
        assert_eq!(volumes[1], "wow/sv05");
        assert_eq!(volumes[2], "bar/sv02");
    }
}
