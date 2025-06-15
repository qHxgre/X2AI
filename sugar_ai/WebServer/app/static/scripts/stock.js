document.addEventListener('DOMContentLoaded', function() {
    fetch('/api/analyzing_stocks')
        .then(response => response.json())
        .then(data => {
            const tableBody = document.getElementById('stocksTableBody');
            
            // 将数据转换为数组并按rank排序
            const sortedData = Object.entries(data)
                .map(([instrument, info]) => ({
                    instrument,
                    ...info,
                    // 确保rank是数字类型，如果不是则设为最大值以保证排在最后
                    rank: typeof info.rank === 'number' ? info.rank : Infinity
                }))
                .sort((a, b) => a.rank - b.rank); // 按rank从小到大排序
            
            // 清空表格（防止重复加载）
            tableBody.innerHTML = '';
            
            // 使用排序后的数据创建行
            sortedData.forEach(item => {
                const { instrument, ...info } = item;
                
                // 创建主行
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${instrument}</td>
                    <td>${info.pe_ttm || 'N/A'}</td>
                    <td>${info.roe_ttm || 'N/A'}</td>
                    <td>${info.net_profit_ttm_yoy || 'N/A'}</td>
                    <td>${info.rank !== Infinity ? info.rank : 'N/A'}</td>
                    <td><span class="recommendation-btn" onclick="toggleRecommendation('${instrument}')">点击查看</span></td>
                `;
                tableBody.appendChild(row);
                
                // 创建推荐理由详情行
                const detailRow = document.createElement('tr');
                detailRow.id = `detail-${instrument}`;
                detailRow.className = 'recommendation-detail';
                const detailCell = document.createElement('td');
                detailCell.colSpan = 6;
                // 使用reason字段，如果不存在则使用recommendation字段，都不存在则显示默认文本
                detailCell.textContent = info.reason || info.recommendation || '暂无推荐理由';
                detailRow.appendChild(detailCell);
                tableBody.appendChild(detailRow);
            });
        })
        .catch(error => {
            console.error('Error fetching stock data:', error);
        });
});

function toggleRecommendation(instrument) {
    const detailRow = document.getElementById(`detail-${instrument}`);
    if (detailRow) {
        detailRow.classList.toggle('expanded');
    } else {
        console.error(`Detail row not found for instrument: ${instrument}`);
    }
}