use log::{debug, info};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf},
    net::TcpStream,
};

pub struct Client {
    reader: ReadHalf<TcpStream>,
    writer: WriteHalf<TcpStream>,
}

impl Client {
    
    pub fn new(reader: ReadHalf<TcpStream>, writer: WriteHalf<TcpStream>) -> Client {
        Client { reader, writer }
    }

}

impl Client {
    pub async fn listen(&mut self) {
        let mut buffer = Vec::with_capacity(1024);

        while let Ok(n) = self.reader.read_buf(&mut buffer).await {
            if n == 0 {
                break;
            }

            let message = String::from_utf8_lossy(&buffer);
            debug!("Received message: {}", message);
            
            
            buffer.clear();
        }
        info!("Connection closed")
    }
}
