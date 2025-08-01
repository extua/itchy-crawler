use rand::Rng;
use std::fs::{self, File};
use std::io::{self, BufRead};
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use reqwest::header::RETRY_AFTER;
use reqwest::{Client, Response, StatusCode};

#[tokio::main]
async fn main() {
    if let Ok(urls) = read_lines("urls") {
        let mut rng = rand::rng();
        let mut delay_ratchet: u64 = 20u64;

        for url in urls.map_while(Result::ok).enumerate() {
            
            let delay: u64 = rng.random_range(delay_ratchet..(delay_ratchet + 20));
            // maintain state by writing line number to file
            let state: usize = fs::read_to_string("state").unwrap().parse().unwrap();
            // skip over line numbers until the last one saved to file
            if url.0 >= state {
                println!("reading from line {state}");
                fs::write("state", url.0.to_string()).unwrap();

                println!("downloading {}", url.1);
                let page_response: (String, bool) = download_page(&url.1).await;
                if page_response.1 == true {
                    delay_ratchet += rng.random_range(5..20)
                }
                println!("sleeping {delay}ms");
                sleep(Duration::from_millis(delay));

                let json_url: String = format!("{}/data.json", &url.1);
                println!("downloading {}", json_url);
                let json_response: (String, bool) = download_page(&json_url).await;
                if json_response.1 == true {
                    delay_ratchet += rng.random_range(5..20)
                }
                println!("sleeping {delay}ms");
                sleep(Duration::from_millis(delay));

                // at this point, we have two resources, a page, and some json
                // parse to a struct?

            } else {
                continue;
            }
        }
    }
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn create_client() -> Client {
    const APP_USER_AGENT: &str = concat!(
        env!("CARGO_PKG_NAME"),
        " Bodleian Libraries Oxford pierre.marshall@bodleian.ox.ac.uk"
    );
    reqwest::Client::builder()
        .user_agent(APP_USER_AGENT)
        .zstd(true)
        .build()
        .unwrap()
}

async fn download_page(url: &String) -> (String, bool) {
    const RETRY_SCALE: [u64; 13] = [1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377];

    let mut retries: usize = 0;

    let client: Client = create_client();

    let response_from_retry: Response = loop {
        match client.get(url).send().await {
            // if response is successful, return the response!
            Ok(resp) if resp.status().is_success() => break resp,
            // if status is 429, back off and retry
            Ok(resp)
                if resp.status() == StatusCode::TOO_MANY_REQUESTS
                    && retries < RETRY_SCALE.len() =>
            {
                // move along the retry scale, set new backoff duration, and sleep
                if let Some(backoff_value) = RETRY_SCALE.into_iter().nth(retries) {
                    let backoff: Duration = Duration::from_secs(backoff_value);
                    println!("Got a 429 error, sleeping {backoff:?} seconds");
                    sleep(backoff);
                }
                retries += 1;
            }
            // get the retry-after header value, convert it
            // to seconds, then to duration, etc.
            Ok(resp)
                if resp.headers().contains_key("retry-after") && retries < RETRY_SCALE.len() =>
            {
                if let Some(retry_after) = resp.headers().get(RETRY_AFTER)
                    && let Ok(retry_after) = retry_after.to_str()
                    && let Ok(retry_after) = retry_after.parse::<u64>()
                    && retry_after < 233
                {
                    let backoff = Duration::from_secs(retry_after + 1);
                    println!("Got a retry-after response, sleeping {backoff:?} seconds");
                    sleep(backoff);
                }
                retries += 1;
            }
            // Breaking out with an error is fine,
            // the last match arm should never be met
            _ => panic!("Network request failed"),
        }
    };

    let retried: bool = if retries != 0 { true } else { false };

    (response_from_retry.text().await.unwrap(), retried)
}
