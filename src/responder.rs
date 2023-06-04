use s3::request::ResponseDataStream;
use axum::{
    response::{IntoResponse, Response}, body::StreamBody, 
};
use crate::cache::{
    ObjectMeta,
    DataObject
};

impl From<ResponseDataStream> for DataObject {
    fn from(s: ResponseDataStream) -> Self {
        DataObject { 
            meta: ObjectMeta::from(&s.headers),
            stream: s.bytes,
            status: Default::default(),
        }
    }
}

impl IntoResponse for DataObject {
    fn into_response(self) -> Response {
        let mut headers = self.meta.headers();
        if let Some(status) = self.status {
            if let Ok(val) = status.parse() {
                headers.insert("x-s3cdn-status", val);
            }
        }
        // make stream response
        (headers, StreamBody::new(self.stream)).into_response()
    }
}
