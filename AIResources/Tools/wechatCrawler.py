import os
from playwright.async_api import async_playwright

image_loading_script = """
let images = document.querySelectorAll('img[data-src]');
images.forEach(img => {
    img.src = img.getAttribute('data-src');
});
"""

image_wait_script = """
let images = document.querySelectorAll('img[data-src]');
new Promise(resolve => {
    let totalImages = images.length;
    let loadedImages = 0;

    // 如果页面没有图片，直接返回
    if (totalImages === 0) {
        resolve();
    }
    
    images.forEach(img => {
        if (img.complete) {
            loadedImages++;
        } else {
            img.onload = img.onerror = function() {
                loadedImages++;
                if (loadedImages === totalImages) {
                    resolve();
                }
            };
        }
    });
});
"""

class wechatCrawler:
    def __init__(self, basedir: str = "./assets") -> None:
        self.basedir = os.path.abspath(os.path.join(os.getcwd(), basedir))
        os.makedirs(self.basedir, exist_ok=True)
        self.playwright = None
        self.browser = None
        self.page = None

    async def __aenter__(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True)
        self.page = await self.browser.new_page()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.browser.close()
        await self.playwright.stop()

    async def url_to_pdf(self, url: str, filename: str) -> None:
        await self.page.goto(url)
        await self.page.evaluate(image_loading_script)
        await self.page.evaluate(image_wait_script)
        if not filename.lower().endswith(".pdf"):
            filename += ".pdf"
        filepath = os.path.join(self.basedir, filename)
        await self.page.pdf(path=filepath)
        print(f"Saved {filename} to {filepath}")