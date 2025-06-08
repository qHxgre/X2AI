document.addEventListener('DOMContentLoaded', function() {
    get_reports();
    get_chart();
});

// 获取并展示图表
async function get_chart() {
    try {
        const response = await fetch('/get_chart');
        const result = await response.json();
        const chartTitle = result.last_date;
        const htmlContent = result.data;
        
        // 创建一个临时div来放置内容
        const tempDiv = document.createElement('div');
        tempDiv.innerHTML = htmlContent;
        
        // 获取图表容器和脚本
        const chartContainer = tempDiv.querySelector('.chart-container');
        const scripts = tempDiv.querySelectorAll('script');
        
        const chartContent = document.getElementById('chartContent');
        chartContent.innerHTML = '';
        
        // 添加标题
        const titleElement = document.createElement('h3');
        titleElement.textContent = `最新日期: (${chartTitle})`;
        chartContent.appendChild(titleElement);
        
        if (chartContainer) {
            chartContent.appendChild(chartContainer);
            
            // 执行所有脚本
            scripts.forEach(script => {
                const newScript = document.createElement('script');
                if (script.src) {
                    newScript.src = script.src;
                } else {
                    newScript.textContent = script.textContent;
                }
                document.body.appendChild(newScript);
            });
        } else {
            chartContent.innerHTML = '<p>无法加载图表</p>';
        }
    } catch (error) {
        console.error('加载图表失败:', error);
        document.getElementById('chartContent').innerHTML = '<p>图表加载失败</p>';
    }
}

// 获取报告
async function get_reports() {
    try {
        const response = await fetch('/get_reports');
        const result = await response.json();
        
        if (result.status === 'success') {
            renderReports(result.data);
        } else {
            console.error('获取报告失败:', result);
        }
    } catch (error) {
        console.error('请求失败:', error);
    }
}

// 展示报告
function renderReports(reports) {
    const container = document.getElementById('reportsContainer');
    container.innerHTML = ''; // 清空现有内容

    // 创建报告列表容器
    const listContainer = document.createElement('div');
    listContainer.className = 'report-list-container';

    // 确保按日期降序排序
    const sortedEntries = Object.entries(reports).sort((a, b) => {
        return new Date(b[0]) - new Date(a[0]); // 降序排序
    });

    // 添加报告项
    sortedEntries.forEach(([date, content]) => {
        const reportItem = document.createElement('div');
        reportItem.className = 'report-item';

        // 创建标题部分
        const header = document.createElement('div');
        header.className = 'report-header';
        header.textContent = date;
        
        // 创建内容部分
        const contentDiv = document.createElement('div');
        contentDiv.className = 'report-content';
        contentDiv.style.display = 'none';
        contentDiv.innerHTML = `
            <div class="markdown-body">
                ${marked.parse(content)}
            </div>
        `;

        // 点击事件处理
        header.addEventListener('click', () => {
            // 如果当前内容已显示，则隐藏
            if (contentDiv.style.display === 'block') {
                contentDiv.style.display = 'none';
                reportItem.classList.remove('active');
            } else {
                // 隐藏所有其他报告内容
                document.querySelectorAll('.report-content').forEach(item => {
                    item.style.display = 'none';
                });
                document.querySelectorAll('.report-item').forEach(item => {
                    item.classList.remove('active');
                });
                
                // 显示当前报告内容
                contentDiv.style.display = 'block';
                reportItem.classList.add('active');
            }
        });

        reportItem.appendChild(header);
        reportItem.appendChild(contentDiv);
        listContainer.appendChild(reportItem);
    });

    container.appendChild(listContainer);
    
    // 默认展开第一个报告
//     if (Object.keys(reports).length > 0) {
//         const firstItem = listContainer.querySelector('.report-item');
//         firstItem.querySelector('.report-header').click();
//     }
}