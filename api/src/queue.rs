#![allow(dead_code)]
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};

pub struct Queue<T> {
    max_len: Option<u64>,
    q: Arc<Mutex<VecDeque<T>>>,
    cvar: Arc<Condvar>,
    in_progress: Arc<Mutex<u64>>,
}

impl<T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Queue {
            max_len: self.max_len.clone(),
            q: self.q.clone(),
            cvar: self.cvar.clone(),
            in_progress: self.in_progress.clone(),
        }
    }
}

impl<T> Queue<T> {
    pub fn new() -> Self {
        Queue {
            max_len: None,
            q: Arc::new(Mutex::new(VecDeque::new())),
            cvar: Arc::new(Condvar::new()),
            in_progress: Arc::new(Mutex::new(0)),
        }
    }
    pub fn with_max_len(mut self, max_len: u64) -> Self {
        self.max_len = Some(max_len);
        self
    }
    pub fn push(&mut self, t: T) {
        let mut queue = self.q.lock().expect("lock");
        if let Some(ref max_len) = self.max_len {
            while queue.len() as u64 >= (*max_len - 1) {
                debug!("Waiting for queue space. len={}", queue.len());
                queue = self.cvar.wait(queue).expect("cvar");
            }
        }
        debug!("Pushing item. len={}", queue.len());
        queue.push_back(t);
        debug!("Pushed item. len={}", queue.len());
        self.cvar.notify_all();
    }
    pub fn try_pop(&mut self) -> Option<QueueItem<T>> {
        let mut queue = self.q.lock().expect("lock");
        if let Some(item) = queue.pop_front() {
            debug!("Popped item. Post pop len={}", queue.len());
            self.cvar.notify_all();
            let mut count = self.in_progress.lock().expect("in_progress lock");
            *count += 1;
            return Some(QueueItem::new(self, item));
        }
        None
    }
    pub fn pop(&mut self) -> QueueItem<T> {
        let mut queue = self.q.lock().expect("lock");
        while queue.is_empty() {
            debug!("Waiting to pop. len={}", queue.len());
            queue = self.cvar.wait(queue).expect("cvar");
        }
        if let Some(item) = queue.pop_front() {
            debug!("Popped item. Post pop len={}", queue.len());
            self.cvar.notify_all();
            let mut count = self.in_progress.lock().expect("in_progress lock");
            *count += 1;
            return QueueItem::new(self, item);
        }
        unreachable!();
    }
    pub fn pop_until_complete(&mut self) -> Option<QueueItem<T>> {
        let mut queue = self.q.lock().expect("lock");
        while !queue.is_empty() || self.in_progress() > 0 {
            debug!("Waiting to pop. len={}", queue.len());
            queue = self.cvar.wait(queue).expect("cvar");
        }
        if let Some(item) = queue.pop_front() {
            debug!("Popped item. Post pop len={}", queue.len());
            self.cvar.notify_all();
            let mut count = self.in_progress.lock().expect("in_progress lock");
            *count += 1;
            return Some(QueueItem::new(self, item));
        }
        unreachable!();
    }
    pub fn len(&self) -> u64 {
        let queue = self.q.lock().expect("lock");
        let count = self.in_progress.lock().expect("in_progress lock");
        queue.len() as u64 + *count
    }
    pub fn in_progress(&self) -> u64 {
        let count = self.in_progress.lock().expect("in_progress lock");
        *count
    }
    pub fn wait(&mut self) {
        let mut queue = self.q.lock().expect("lock");
        while !queue.is_empty() {
            debug!("Waiting for queue empty. len={}", queue.len());
            queue = self.cvar.wait(queue).expect("cvar");
        }
        debug!("Queue is empty");
    }
}

// QueueItem

pub struct QueueItem<T> {
    t: Option<T>,
    in_progress: Arc<Mutex<u64>>,
    success: bool,
}

impl<T> QueueItem<T> {
    fn new(queue: &Queue<T>, t: T) -> Self {
        QueueItem {
            t: Some(t),
            in_progress: queue.in_progress.clone(),
            success: false,
        }
    }
    pub fn take(&mut self) -> T {
        self.t.take().expect("Already taken")
    }
    pub fn success(&mut self) {
        self.success = true;
    }
}

impl<T> Drop for QueueItem<T> {
    fn drop(&mut self) {
        let mut count = self.in_progress.lock().expect("in_progress lock");
        *count -= 1;

        if self.success {
            trace!("Drop with success");
        } else {
            warn!("Drop NO success");
        }
    }
}

#[cfg(test)]
mod test {
    extern crate env_logger;
    use std::thread;
    use super::*;

    #[test]
    fn push_pop_single_thread_single_item() {
        let _ = env_logger::init();

        let mut queue = Queue::new();
        assert_eq!(0, queue.len());
        {
            queue.push(0);
            assert_eq!(1, queue.len());
            let mut x = queue.pop();
            assert_eq!(0, x.take());
            x.success();
        }
        assert_eq!(0, queue.in_progress());
        assert_eq!(0, queue.len());
    }

    #[test]
    fn push_pop_single_thread_multi_item() {
        let _ = env_logger::init();

        let mut queue = Queue::new();
        for _i in 0..10 {
            queue.push(0);
        }
        for _i in 0..10 {
            {
                let mut x = queue.pop();
                x.success();
            }
            assert_eq!(0, queue.in_progress());
        }
        assert_eq!(0, queue.in_progress());
        assert_eq!(0, queue.len());
    }

    #[test]
    fn push_pop_2_thread() {
        let _ = env_logger::init();

        let thread_push;
        let thread_pop;

        let mut queue = Queue::new();
        {
            let mut queue = queue.clone();
            thread_push = thread::spawn(move || {
                for _i in 0..1000 {
                    queue.push(0);
                }
            });
        }
        {
            let mut queue = queue.clone();
            thread_pop = thread::spawn(move || {
                for _i in 0..1000 {
                    let mut x = queue.pop();
                    x.success();
                }
            });
        }

        thread_push.join().expect("push_join");
        thread_pop.join().expect("pop_join");

        assert_eq!(0, queue.in_progress());
        assert_eq!(0, queue.len());
    }

    #[test]
    fn pop_drain_multi_thread() {
        let _ = env_logger::init();

        let mut queue = Queue::new();
        {
            let mut queue = queue.clone();
            for _ in 0..10000 {
                queue.push(0);
            }
        }
        {
            for _ in 0..10 {
                let mut queue = queue.clone();
                thread::spawn(move || {
                    for _i in 0..1000 {
                        let mut x = queue.pop();
                        x.success();
                    }
                });
            }
        }

        queue.wait();
        assert_eq!(0, queue.in_progress());
        assert_eq!(0, queue.len());
    }

    #[test]
    fn pop_drain_multi_thread_with_max_len() {
        let _ = env_logger::init();

        let thread_push_a;
        let thread_push_b;
        let thread_pop;

        let mut queue: Queue<u64> = Queue::new().with_max_len(2);
        {
            let mut queue = queue.clone();
            thread_push_a = thread::spawn(move || {
                for _i in 0..500 {
                    queue.push(0);
                    assert!(queue.len() <= 2);
                }
            });
        }
        {
            let mut queue = queue.clone();
            thread_push_b = thread::spawn(move || {
                for _i in 0..500 {
                    queue.push(0);
                    assert!(queue.len() <= 2);
                }
            });
        }
        {
            let mut queue = queue.clone();
            thread_pop = thread::spawn(move || {
                for _i in 0..1000 {
                    {
                        let mut x = queue.pop();
                        x.success();
                    }
                    assert!(queue.len() <= 2);
                }
            });
        }

        thread_push_a.join().expect("push_join");
        thread_push_b.join().expect("push_join");
        thread_pop.join().expect("pop_join");
        assert_eq!(0, queue.in_progress());
        assert_eq!(0, queue.len());
    }

}
