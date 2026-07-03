//! API-specific browser lifecycle management.
//!
//! The API keeps a single shared Selenium/Chrome browser behind a Tokio mutex.
//! This serializes scraper access while avoiding one ChromeDriver process per
//! request. `AniWorldSerienStream` and the related series downloaders require a
//! `thirtyfour::WebDriver` because AniWorld/SerienStream render the relevant
//! page contents in a real browser/Selenium session.

use std::process::Child;

use anyhow::Context;
use futures_util::future::LocalBoxFuture;
use tokio::sync::Mutex;

use crate::api::sdl_wrapper::api_log_wrapper;
use crate::{chrome, dirs, download};

/// Shared browser manager for API requests.
pub struct ApiBrowser {
    inner: Mutex<Option<ManagedBrowser>>,
}

struct ManagedBrowser {
    driver: thirtyfour::WebDriver,
    chromedriver: Child,
}

impl std::fmt::Debug for ApiBrowser {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ApiBrowser").finish_non_exhaustive()
    }
}

impl ApiBrowser {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }

    /// Run an operation with the shared API WebDriver.
    ///
    /// Access is serialized by the manager's mutex because the downloader stack
    /// navigates pages and executes scripts on a single browser session.
    pub async fn with_driver<T, F>(&self, operation: F) -> Result<T, anyhow::Error>
    where
        F: for<'driver> FnOnce(&'driver thirtyfour::WebDriver) -> LocalBoxFuture<'driver, Result<T, anyhow::Error>>,
    {
        let mut guard = self.inner.lock().await;
        if guard.is_none() {
            *guard = Some(Self::start_browser().await?);
        }

        let browser = guard.as_ref().context("api browser is unavailable")?;
        operation(&browser.driver).await
    }

    pub async fn get_user_agent(&self) -> Result<Option<String>, anyhow::Error> {
        self.with_driver(|driver| Box::pin(async move { Ok(chrome::get_user_agent(driver).await) }))
            .await
    }

    /// Shut down Selenium cleanly and terminate the ChromeDriver process.
    pub async fn shutdown(&self) {
        let Some(mut browser) = self.inner.lock().await.take() else {
            return;
        };

        if let Err(err) = browser.driver.quit().await {
            log::warn!("Failed to quit API browser session: {err:#}");
        }
        if let Err(err) = browser.chromedriver.kill() {
            log::warn!("Failed to terminate API ChromeDriver process: {err}");
        }
        let _ = browser.chromedriver.wait();
    }

    async fn start_browser() -> Result<ManagedBrowser, anyhow::Error> {
        let data_dir = dirs::get_data_dir().await?;
        let limiter = async_speed_limit::Limiter::new(f64::INFINITY);
        let asset_downloader = {
            let mut log_wrapper = api_log_wrapper()?;
            download::Downloader::new(
                log_wrapper.as_mut().expect("api log wrapper initialized"),
                limiter,
                false,
                None,
                None,
                None,
            )
        };
        let (driver, chromedriver) = chrome::ChromeDriver::get(&data_dir, &asset_downloader, true).await?;
        Ok(ManagedBrowser { driver, chromedriver })
    }
}

impl Default for ApiBrowser {
    fn default() -> Self {
        Self::new()
    }
}
