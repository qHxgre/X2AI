// Initialize date picker with current date
document.getElementById('currentDate').value = new Date().toISOString().split('T')[0];

// DOM elements
const datePicker = document.getElementById('currentDate');
const fetchDataBtn = document.getElementById('fetchDataBtn');
const chartContent = document.getElementById('chartContent');
const reportContent = document.getElementById('reportContent');
const instructions = document.querySelector('.instructions');

// Event listeners
document.addEventListener('DOMContentLoaded', function() {
    // 获取K线图
    fetchKlines();
    
    // 获取分析报告
    fetchReports();

    // Setup event listeners
    fetchDataBtn.addEventListener('click', fetchReports);
});

// Fetch K-line data
async function fetchKlines() {
    chartContent.innerHTML = '<p class="placeholder">正在加载图表数据...</p>';

    try {
        const response = await fetch('/api/get_klines');
        
        const result = await response.json();
        if (result.status === 'success') {
            renderKlines(result.data);
        } else {
            throw new Error(result.message || '获取K线数据失败');
        }

    } catch (error) {
        console.error('Error fetching klines:', error);
        chartContent.innerHTML = `<p class="error">图表数据加载失败: ${error.message}</p>`;
    }
}

function renderKlines(htmlContent) {
    try {
        // 创建一个临时div来放置内容
        const tempDiv = document.createElement('div');
        tempDiv.innerHTML = htmlContent;
        
        // 获取图表容器和脚本
        const chartContainer = tempDiv.querySelector('.chart-container') || tempDiv;
        const scripts = tempDiv.querySelectorAll('script');
        
        // 清空现有内容
        chartContent.innerHTML = '';
        
        // 添加图表内容
        chartContent.appendChild(chartContainer.cloneNode(true));
        
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
    } catch (error) {
        console.error('Error rendering klines:', error);
        chartContent.innerHTML = '<p class="error">图表渲染失败</p>';
    }
}

// Fetch reports data
async function fetchReports() {
    reportContent.innerHTML = '<p class="placeholder">正在加载分析报告...</p>';
    
    // Get current selections
    const date = datePicker.value;
    
    // Validate date
    if (!date) {
        throw new Error('请选择有效日期');
    }
    
    // Prepare payload
    const payload = {
        date: date
    };

    try {
        const response = await fetch('/api/get_reports', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload)
        });
        
        const result = await response.json();
        if (result.status === 'success') {
            renderReports(result.data);
        } else {
            throw new Error(result.message || '获取报告失败');
        }
    } catch (error) {
        console.error('Error fetching reports:', error);
        reportContent.innerHTML = `<p class="error">报告加载失败: ${error.message}</p>`;
    }
}

// 展示报告
function renderReports(content) {
    try {
        reportContent.innerHTML = `
            <div class="markdown-body">
                ${marked.parse(content)}
            </div>
        `;
    } catch (error) {
        console.error('Error rendering reports:', error);
        reportContent.innerHTML = '<p class="error">报告渲染失败</p>';
    }
}