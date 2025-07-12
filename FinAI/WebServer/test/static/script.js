// 初始化加载首页
document.addEventListener('DOMContentLoaded', () => {
    // 从URL参数获取当前页面（例如 ?page=stocks）
    const urlParams = new URLSearchParams(window.location.search);
    const defaultPage = urlParams.get('page') || 'home';
    loadPage(defaultPage);

    // 导航点击事件
    document.querySelectorAll('nav a').forEach(link => {
        link.addEventListener('click', (e) => {
            e.preventDefault();
            const page = e.target.getAttribute('data-page');
            loadPage(page);
            
            // 更新URL（不刷新页面）
            history.pushState({ page }, '', `?page=${page}`);
        });
    });

    // 处理浏览器前进/后退
    window.addEventListener('popstate', (e) => {
        const page = e.state?.page || 'home';
        loadPage(page);
    });
});

// 加载页面内容
async function loadPage(page) {
    try {
        // 1. 加载HTML内容
        const response = await fetch(`${page}.html`);
        if (!response.ok) throw new Error('页面不存在');
        const html = await response.text();
        
        // 2. 更新DOM
        document.getElementById('content-container').innerHTML = html;
        document.title = `${page} - XAI金融分析平台`;

        // 3. 更新导航栏高亮
        document.querySelectorAll('nav a').forEach(link => {
            link.classList.toggle('active', link.getAttribute('data-page') === page);
        });

    } catch (err) {
        console.error('加载失败:', err);
        document.getElementById('content-container').innerHTML = `
            <h1>404</h1>
            <p>页面加载失败，请<a href="?page=home">返回首页</a></p>
        `;
    }
}