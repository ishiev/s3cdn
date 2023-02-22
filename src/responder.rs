use rocket::{
    response::{Responder, self}, 
    Request, Response
};
use s3::request::ResponseDataStream;
use tokio_util::io::StreamReader;

pub struct DataStreamResponder {
    stream: ResponseDataStream, 
}

impl From<ResponseDataStream> for DataStreamResponder {
    fn from(s: ResponseDataStream) -> Self {
        DataStreamResponder { stream: s }
    }
}

impl<'r> Responder<'r, 'static> for DataStreamResponder {
    fn respond_to(self, _: &'r Request<'_>) -> response::Result<'static> {
        let reader = StreamReader::new(self.stream.bytes);
        Response::build()
            .streamed_body(reader)
            .ok()
    }
}