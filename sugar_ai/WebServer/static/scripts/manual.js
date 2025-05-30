// 初始化日期
document.getElementById('startDate').value =
    new Date(Date.now() - 7 * 86400000).toISOString().split('T')[0];
document.getElementById('endDate').value =
    new Date().toISOString().split('T')[0];

// 在全局保存当前文章数据
let currentArticles = [];
// 在全局保存控制器
let abortController = null;


// 获取文章
async function get_article() {
    const getArticleBtn = document.getElementById('getArticleBtn');
    const payload = {
        startDate: document.getElementById('startDate').value,
        endDate: document.getElementById('endDate').value,
    };

    try {
        // 禁用按钮
        getArticleBtn.disabled = true;
        getArticleBtn.textContent = '获取中...';
        getArticleBtn.style.opacity = '0.7';
        getArticleBtn.style.cursor = 'not-allowed';

        const response = await fetch('/get_article', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload)
        });
        
        const result = await response.json();
        if (result.status === 'success') {
            renderNews(result.data);
        }
        
    } catch (error) {
        console.error('请求失败:', error);
    } finally {
        // 无论成功或失败都恢复按钮
        getArticleBtn.disabled = false;
        getArticleBtn.textContent = '获取文章';
        getArticleBtn.style.opacity = '1';
        getArticleBtn.style.cursor = 'pointer';
    }
}



// 展示文章
function renderNews(articles) {
    currentArticles = articles; // 保存文章数据
    const container = document.getElementById('newsContainer');
    container.innerHTML = ''; // 清空现有内容

    articles.forEach(article => {
        const newsItem = document.createElement('div');
        newsItem.className = 'news-item';
        
        // 新闻标题头
        const header = document.createElement('div');
        header.className = 'news-header';
        header.innerHTML = `
            <div style="flex-grow: 1">${article.title}</div>
        `;

        // 新闻内容区
        const content = document.createElement('div');
        content.className = 'news-content';
        content.innerHTML = `
            <div class="markdown-body">
                ${article.content}
            </div>
        `;

        // 点击交互
        header.addEventListener('click', () => {
            // 关闭其他所有新闻项
            document.querySelectorAll('.news-item').forEach(item => {
                if (item !== newsItem) {
                    item.classList.remove('active');
                    item.querySelector('.news-content').classList.remove('active');
                }
            });
            
            // 切换当前项状态
            newsItem.classList.toggle('active');
            content.classList.toggle('active');
        });

        newsItem.append(header, content);
        container.appendChild(newsItem);
    });
}



// AI 分析
async function handleStart() {
    const analysisBtn = document.getElementById('analysisBtn');
    
    // 如果正在进行中，则中止分析
    if (abortController) {
        handleAbort();
        return;
    }
    
    if (!currentArticles.length) {
        alert('请先获取文章');
        return;
    }
    
    // 创建新的AbortController用于中断请求
    abortController = new AbortController();
    const progressDiv = document.getElementById('progress');
    const analysisContent = document.getElementById('analysisContent');
    
    try {
        // 更新按钮状态
        analysisBtn.textContent = '中止分析';
        analysisBtn.style.backgroundColor = '#dc3545'; // 红色表示中止
        
        const payload = {
            articles: currentArticles,
            // userOpinion: document.getElementById('userOpinion').value
        };

        const response = await fetch('/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
            signal: abortController.signal
        });

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = '';

        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            
            buffer += decoder.decode(value, { stream: true });
            const parts = buffer.split('\n');
            
            buffer = parts.pop(); // 保留未完成部分
            
            for (const part of parts) {
                if (part.trim() === '') continue;
                try {
                    const data = JSON.parse(part);
                    
                    if (data.status === 'processing') {
                        progressDiv.innerHTML = `
                            <div class="progress-info highlight">
                                正在分析：${data.current}/${data.total} 
                                标题：${data.title}
                            </div>
                        `;
                    }
                    else if (data.status === 'success') {
                        analysisContent.innerHTML = data.report;
                        progressDiv.innerHTML = '<div class="progress-info">分析完成！</div>';
                    }
                } catch (e) {
                    console.error('解析错误:', e);
                }
            }
        }
    } catch (error) {
        if (error.name === 'AbortError') {
            progressDiv.innerHTML = '<div class="progress-info">用户中止了分析</div>';
        } else {
            console.error('分析失败:', error);
            progressDiv.innerHTML = `<div class="progress-info">分析失败: ${error.message}</div>`;
        }
    } finally {
        // 恢复按钮状态
        analysisBtn.textContent = 'AI分析';
        analysisBtn.style.backgroundColor = ''; // 恢复默认颜色
        abortController = null;
    }
}

// 添加中止按钮功能
function handleAbort() {
    if (abortController) {
        abortController.abort();
    }
}
