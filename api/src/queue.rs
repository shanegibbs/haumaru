#![allow(dead_code)]
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};

pub struct Queue<T> {
    name: String,
    max_len: Option<u64>,
    q: Arc<Mutex<QueueState<T>>>,
    cvar: Arc<Condvar>,
}

impl<T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Queue {
            name: self.name.clone(),
            max_len: self.max_len.clone(),
            q: self.q.clone(),
            cvar: self.cvar.clone(),
        }
    }
}

struct QueueState<T> {
    q: VecDeque<T>,
    in_progress: u64,
}

impl<T> Queue<T> {
    pub fn new(name: &str) -> Self {
        Queue {
            name: name.into(),
            max_len: None,
            q: Arc::new(Mutex::new(QueueState {
                q: VecDeque::new(),
                in_progress: 0,
            })),
            cvar: Arc::new(Condvar::new()),
        }
    }
    pub fn with_max_len(mut self, max_len: u64) -> Self {
        self.max_len = Some(max_len);
        self
    }
    pub fn push(&mut self, t: T) {
        let mut state = self.q.lock().expect("lock");
        if let Some(ref max_len) = self.max_len {
            while state.q.len() as u64 >= (*max_len - 1) {
                debug!("({}) Waiting for queue space. len={}",
                       self.name,
                       state.q.len());
                state = self.cvar.wait(state).expect("cvar");
            }
        }
        debug!("({}) Pushing item. len={}", self.name, state.q.len());
        state.q.push_back(t);
        debug!("({}) Pushed item. len={}", self.name, state.q.len());
        self.cvar.notify_all();
    }
    pub fn try_pop(&mut self) -> Option<QueueItem<T>> {
        let mut state = self.q.lock().expect("lock");
        if let Some(item) = state.q.pop_front() {
            debug!("({}) Popped item. Post pop len={}",
                   self.name,
                   state.q.len());
            state.in_progress += 1;
            self.cvar.notify_all();
            return Some(QueueItem::new(self, item));
        }
        None
    }
    pub fn pop(&mut self) -> QueueItem<T> {
        let mut state = self.q.lock().expect("lock");
        while state.q.is_empty() {
            debug!("({}) Waiting to pop", self.name);
            state = self.cvar.wait(state).expect("cvar");
        }
        if let Some(item) = state.q.pop_front() {
            state.in_progress += 1;
            debug!("({}) Popped item. Post pop len={}, in_progress={}",
                   self.name,
                   state.q.len(),
                   state.in_progress);
            self.cvar.notify_all();
            return QueueItem::new(self, item);
        }
        unreachable!();
    }
    pub fn pop_until_complete(&mut self) -> Option<QueueItem<T>> {
        let mut state = self.q.lock().expect("lock");
        while !state.q.is_empty() || self.in_progress() > 0 {
            debug!("({}) Waiting to pop. len={}", self.name, state.q.len());
            state = self.cvar.wait(state).expect("cvar");
        }
        if let Some(item) = state.q.pop_front() {
            debug!("({}) Popped item. Post pop len={}",
                   self.name,
                   state.q.len());
            state.in_progress += 1;
            self.cvar.notify_all();
            return Some(QueueItem::new(self, item));
        }
        unreachable!();
    }
    pub fn len(&self) -> u64 {
        let state = self.q.lock().expect("lock");
        state.in_progress + state.q.len() as u64
    }
    pub fn in_progress(&self) -> u64 {
        let state = self.q.lock().expect("lock");
        state.in_progress
    }
    pub fn wait(&mut self) {
        let mut state = self.q.lock().expect("lock");
        while !state.q.is_empty() || state.in_progress != 0 {
            debug!("({}) Waiting for queue empty. len={}, in_progress={}",
                   self.name,
                   state.q.len(),
                   state.in_progress);
            state = self.cvar.wait(state).expect("cvar");
        }
        debug!("({}) Queue is empty", self.name);
    }
}

// QueueItem

pub struct QueueItem<T> {
    t: Option<T>,
    state: Arc<Mutex<QueueState<T>>>,
    cvar: Arc<Condvar>,
    success: bool,
}

impl<T> QueueItem<T> {
    fn new(queue: &Queue<T>, t: T) -> Self {
        QueueItem {
            t: Some(t),
            state: queue.q.clone(),
            cvar: queue.cvar.clone(),
            success: false,
        }
    }
    pub fn success(&mut self) -> T {
        self.success = true;
        self.t.take().expect("Already taken")
    }
}

impl<T> AsRef<T> for QueueItem<T> {
    fn as_ref(&self) -> &T {
        self.t.as_ref().expect("Already taken")
    }
}

impl<T> AsMut<T> for QueueItem<T> {
    fn as_mut(&mut self) -> &mut T {
        self.t.as_mut().expect("Already taken")
    }
}

impl<T> Drop for QueueItem<T> {
    fn drop(&mut self) {
        let mut state = self.state.lock().expect("state lock");
        state.in_progress -= 1;

        if self.success {
            trace!("Drop with success");
        } else {
            if self.t.is_some() {
                warn!("Drop NO success. Adding to back of queue.");
                state.q.push_back(self.t.take().unwrap());
            } else {
                warn!("Drop NO success");
            }
        }

        self.cvar.notify_all();
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

        let mut queue = Queue::new("test");
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

        let mut queue = Queue::new("test");
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

        let queue = Queue::new("test");
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

        let mut queue = Queue::new("test");
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

        let queue: Queue<u64> = Queue::new("test").with_max_len(2);
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
