use std::sync::Arc;

use anyhow::Context as _;
use clap::Parser;
use futures::StreamExt as _;
use tokio::task::JoinSet;
use url::Url;

mod bindings {
    wit_bindgen_wrpc::generate!({
        with: {
            "wrpc-examples:hello/handler": generate
        }
    });
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// NATS.io URL to connect to
    #[arg(short, long, default_value = "nats://127.0.0.1:4222")]
    nats: Url,

    /// Prefixes to invoke `wrpc-examples:hello/handler.hello` on
    #[arg(default_value = "rust")]
    prefix: String,

    /// Subject to subscribe on for invocations
    #[arg(default_value = "invocation.send")]
    subject: String,

    /// Queue group to listen on for invocations
    #[arg(default_value = "invocations")]
    queue: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let Args {
        nats,
        prefix,
        subject,
        queue,
    } = Args::parse();

    let nats = std::sync::Arc::new(
        async_nats::connect_with_options(
            String::from(nats),
            async_nats::ConnectOptions::new().retry_on_initial_connect(),
        )
        .await
        .context("failed to connect to NATS.io server")?,
    );

    let mut sub = nats
        .queue_subscribe(subject, queue)
        .await
        .context("failed to subscribe")?;
    let mut tasks = JoinSet::new();

    let prefix: Arc<str> = Arc::from(prefix);
    loop {
        if let Some(msg) = sub.next().await {
            let prefix = prefix.clone();
            let nats = nats.clone();
            tasks.spawn(async move {
                let wrpc = wrpc_transport_nats::Client::new(nats.clone(), prefix, None)
                    .await
                    .expect("failed to construct transport client");
                let hello = bindings::wrpc_examples::hello::handler::hello(&wrpc, None)
                    .await
                    .expect("failed to invoke `wrpc-examples.hello/handler.hello`");
                if let Some(reply) = msg.reply {
                    nats.publish(reply, hello.into())
                        .await
                        .expect("failed to publish");
                }
            });
        }
    }
}
