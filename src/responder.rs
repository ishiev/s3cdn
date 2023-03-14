use std::marker::PhantomData;

use rocket::{
    response::{Responder, Response, Result},
    Request, 
};
use s3::request::ResponseDataStream;
use tokio_util::io::StreamReader;

use crate::cache::{
    ObjectCache, 
    ObjectMeta
};

pub struct DataStreamResponder {
    inner: ResponseDataStream, 
}

impl From<ResponseDataStream> for DataStreamResponder {
    fn from(s: ResponseDataStream) -> Self {
        DataStreamResponder { inner: s }
    }
}

impl<'r> Responder<'r, 'static> for DataStreamResponder {
    fn respond_to(self, _: &'r Request<'_>) -> Result<'static> {
        let reader = StreamReader::new(self.inner.bytes);
        let mut builder = Response::build();
        // add nessesary headers
        let meta = ObjectMeta::from(&self.inner.headers);
        for h in meta.headers().into_iter() {
            builder.header(h);
        }
        builder
            .streamed_body(reader)
            .ok()
    }
}

pub struct CacheResponder<'r, 'o: 'r, R: Responder<'r, 'o>> {
    inner: R,
    cache: &'r ObjectCache,
    #[doc(hidden)]
    _phantom: PhantomData<(&'r R, &'o R)>,
}

impl<'r, 'o: 'r, R: Responder<'r, 'o>> Responder<'r, 'o> for CacheResponder<'r, 'o, R> {
    fn respond_to(self, req: &'r Request<'_>) -> Result<'o> {
        let mut res = Response::build_from(self.inner.respond_to(req)?);
        for h in self.cache.headers().into_iter() {
            res.header(h);
        }
        res.ok()
    }
}

impl <'r, 'o: 'r, R: Responder<'r, 'o>> CacheResponder<'r, 'o, R> {
    pub fn new(inner: R, cache: &'r ObjectCache) -> Self {
        Self {
            inner,
            cache,
            _phantom: PhantomData
        }
    }
}
