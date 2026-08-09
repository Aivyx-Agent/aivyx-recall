//! Shared behavior contract, run against every `Recall` implementation in
//! this crate (`in_memory.rs`'s tests today; `file.rs`'s tests once Task 2
//! lands). See `Recall`'s own doc comment for what's being asserted here.

use crate::{Recall, RecallError};

pub(crate) async fn assert_conformance(recall: &dyn Recall) {
    // Empty-topic errors on all three methods.
    assert!(matches!(recall.put("", "x").await, Err(RecallError::EmptyTopic)));
    assert!(matches!(
        recall.get_recent("", 1).await,
        Err(RecallError::EmptyTopic)
    ));
    assert!(matches!(recall.forget("").await, Err(RecallError::EmptyTopic)));

    // Zero limit.
    assert!(matches!(
        recall.get_recent("t", 0).await,
        Err(RecallError::ZeroLimit)
    ));

    // Unwritten topic returns an empty Vec, not an error.
    assert_eq!(recall.get_recent("unwritten-topic", 10).await.unwrap(), vec![]);

    // seq is monotonic per topic; get_recent orders newest first.
    let seq0 = recall.put("topic-a", "first").await.unwrap();
    let seq1 = recall.put("topic-a", "second").await.unwrap();
    assert_eq!(seq1, seq0 + 1);
    let recent = recall.get_recent("topic-a", 10).await.unwrap();
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0].body, "second");
    assert_eq!(recent[0].seq, seq1);
    assert_eq!(recent[1].body, "first");
    assert_eq!(recent[1].seq, seq0);

    // limit caps results, keeping the newest.
    let limited = recall.get_recent("topic-a", 1).await.unwrap();
    assert_eq!(limited.len(), 1);
    assert_eq!(limited[0].body, "second");

    // forget returns the count deleted and clears the topic.
    let deleted = recall.forget("topic-a").await.unwrap();
    assert_eq!(deleted, 2);
    assert_eq!(recall.get_recent("topic-a", 10).await.unwrap(), vec![]);

    // Forgetting an already-empty topic is a no-op, not an error.
    assert_eq!(recall.forget("topic-a").await.unwrap(), 0);

    // Topics are independent.
    recall.put("topic-b", "b-entry").await.unwrap();
    assert_eq!(recall.get_recent("topic-a", 10).await.unwrap(), vec![]);
    assert_eq!(recall.get_recent("topic-b", 10).await.unwrap().len(), 1);
}
