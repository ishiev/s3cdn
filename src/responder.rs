use rocket::{
    response::{Responder, self}, 
    Request, Response
};
use s3::request::ResponseDataStream;
use tokio_util::io::StreamReader;

pub struct DataStreamResponder {
    inner: ResponseDataStream, 
}

impl From<ResponseDataStream> for DataStreamResponder {
    fn from(s: ResponseDataStream) -> Self {
        DataStreamResponder { inner: s }
    }
}

impl<'r> Responder<'r, 'static> for DataStreamResponder {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        let reader = StreamReader::new(self.inner.bytes);
        let mut builder = Response::build();
        // todo: add only nessesary headers
        for (header_name, header_value) in self.inner.headers.into_iter() {
            builder.raw_header(header_name, header_value);
        }
        builder
            .streamed_body(reader)
            .ok()
    }
}