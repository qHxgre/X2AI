document.addEventListener('DOMContentLoaded', function() {
    // 加载图片
    async function loadImages() {
        try {
            const response = await fetch('/api/get_images');
            if (!response.ok) throw new Error('图片加载失败');
            
            const data = await response.json();
            const imageGallery = document.getElementById('imageGallery');
            
            if (data.images && data.images.length > 0) {
                imageGallery.innerHTML = data.images.map(image => `
                    <div class="image-item">
                        <img src="${image.url}" alt="${image.name}">
                        <div class="image-caption">${image.name}</div>
                    </div>
                `).join('');
            } else {
                imageGallery.innerHTML = '<p>没有找到图片</p>';
            }
        } catch (error) {
            console.error('加载图片时出错:', error);
            document.getElementById('imageGallery').innerHTML = 
                '<p class="error-message">加载图片时出错，请稍后再试</p>';
        }
    }

    
    // 加载Markdown内容
    async function loadMarkdown() {
        try {
            const response = await fetch('/api/get_markdown');
            if (!response.ok) throw new Error('文档加载失败');
            
            const data = await response.json();
            const markdownContent = document.getElementById('markdownContent');
            
            if (data.content) {
                markdownContent.innerHTML = marked.parse(data.content);
            } else {
                markdownContent.innerHTML = '<p>没有找到文档内容</p>';
            }
        } catch (error) {
            console.error('加载文档时出错:', error);
            document.getElementById('markdownContent').innerHTML = 
                '<p class="error-message">加载文档时出错，请稍后再试</p>';
        }
    }

    // 初始化加载内容
    loadImages();
    loadMarkdown();
});