use std::sync::Arc;

use cubesql::{transport::CubeReadStream, CubeError};
#[cfg(build = "debug")]
use log::trace;
use neon::prelude::*;

use crate::utils::bind_method;

use tokio::sync::mpsc::{
    channel as mpsc_channel,
    error::{TryRecvError, TrySendError},
    Receiver, Sender,
};

type Chunk = Result<String, CubeError>;

#[derive(Debug)]
pub struct StreamReader {
    receiver: Receiver<Chunk>,
}

impl Iterator for StreamReader {
    type Item = Chunk;

    fn next(&mut self) -> Option<Self::Item> {
        let poll_wait = std::time::Duration::from_millis(50);
        loop {
            match self.receiver.try_recv() {
                Ok(res) => return Some(res),
                Err(err) => match err {
                    TryRecvError::Empty => {
                        std::thread::sleep(poll_wait);
                    }
                    TryRecvError::Disconnected => return None,
                },
            }
        }
    }
}
pub struct JsWriteStream {
    sender: Sender<Chunk>,
}

impl Finalize for JsWriteStream {}

impl JsWriteStream {
    #[allow(clippy::wrong_self_convention)]
    fn to_object<'a, C: Context<'a>>(self, cx: &mut C) -> JsResult<'a, JsObject> {
        let obj = cx.empty_object();
        // Pass JsAsyncChannel as this, because JsFunction cannot use closure (fn with move)
        let obj_this = cx.boxed(self).upcast::<JsValue>();

        let chunk_fn = JsFunction::new(cx, js_stream_push_chunk)?;
        let chunk = bind_method(cx, chunk_fn, obj_this)?;
        obj.set(cx, "chunk", chunk)?;

        let end_fn = JsFunction::new(cx, js_stream_end)?;
        let end_stream = bind_method(cx, end_fn, obj_this)?;
        obj.set(cx, "end", end_stream)?;

        let reject_fn = JsFunction::new(cx, js_stream_reject)?;
        let reject = bind_method(cx, reject_fn, obj_this)?;
        obj.set(cx, "reject", reject)?;

        Ok(obj)
    }

    fn push_chunk(&self, chunk: String) -> bool {
        let poll_wait = std::time::Duration::from_millis(100);
        let mut attempts = 0;

        loop {
            match self.sender.try_send(Ok(chunk.clone())) {
                Err(err) => match err {
                    TrySendError::Full(_) => {
                        attempts += 1;
                        if attempts >= 50 {
                            return false;
                        }
                        std::thread::sleep(poll_wait);
                    }
                    TrySendError::Closed(_) => {
                        return false;
                    }
                },
                Ok(_) => {
                    return true;
                }
            }
        }
    }

    fn end(&self) {
        self.push_chunk("".to_string());
    }

    fn reject(&self, err: String) {
        let poll_wait = std::time::Duration::from_millis(100_u64);
        let mut attempts = 0;

        while let Err(TrySendError::Full(_)) =
            self.sender.try_send(Err(CubeError::internal(err.clone())))
        {
            attempts += 1;
            if attempts >= 50 {
                break;
            }
            std::thread::sleep(poll_wait);
        }
    }
}

fn js_stream_push_chunk(mut cx: FunctionContext) -> JsResult<JsBoolean> {
    #[cfg(build = "debug")]
    trace!("JsWriteStream.push_chunk");

    let this = cx
        .this()
        .downcast_or_throw::<JsBox<JsWriteStream>, _>(&mut cx)?;
    let result = cx.argument::<JsString>(0)?;
    let result = this.push_chunk(result.value(&mut cx));

    Ok(cx.boolean(result))
}

fn js_stream_end(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsWriteStream.end");

    let this = cx
        .this()
        .downcast_or_throw::<JsBox<JsWriteStream>, _>(&mut cx)?;
    this.end();

    Ok(cx.undefined())
}

fn js_stream_reject(mut cx: FunctionContext) -> JsResult<JsUndefined> {
    #[cfg(build = "debug")]
    trace!("JsWriteStream.reject");

    let this = cx
        .this()
        .downcast_or_throw::<JsBox<JsWriteStream>, _>(&mut cx)?;
    let result = cx.argument::<JsString>(0)?;
    this.reject(result.value(&mut cx));

    Ok(cx.undefined())
}

pub fn call_js_with_stream_as_callback(
    channel: Arc<Channel>,
    js_method: Arc<Root<JsFunction>>,
    query: Option<String>,
) -> Result<Box<CubeReadStream>, CubeError> {
    let (sender, receiver) = mpsc_channel::<Chunk>(100);

    channel.send(move |mut cx| {
        // https://github.com/neon-bindings/neon/issues/672
        let method = match Arc::try_unwrap(js_method) {
            Ok(v) => v.into_inner(&mut cx),
            Err(v) => v.as_ref().to_inner(&mut cx),
        };

        let stream = JsWriteStream { sender };

        let this = cx.undefined();
        let args: Vec<Handle<_>> = vec![
            if let Some(q) = query {
                cx.string(q).upcast::<JsValue>()
            } else {
                cx.null().upcast::<JsValue>()
            },
            stream.to_object(&mut cx)?.upcast::<JsValue>(),
        ];
        method.call(&mut cx, this, args)?;

        Ok(())
    });

    Ok(Box::new(StreamReader { receiver }))
}
