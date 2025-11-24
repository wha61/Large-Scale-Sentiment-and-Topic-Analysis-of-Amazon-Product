# 快速开始指南

## 启动Dashboard

### Windows用户
双击运行：
```
src/web_dashboard/run_dashboard.bat
```

或者命令行：
```bash
cd src/web_dashboard
python app.py
```

### Linux/Mac用户
```bash
cd src/web_dashboard
python app.py
```

## 访问Dashboard

打开浏览器，访问：
```
http://localhost:5000
```

## 功能说明

### 1. 统计概览
- 总评论数
- 不匹配评论数
- 主题数量
- 平均MAE

### 2. 评分vs情感分析
- **柱状图**: 各评分的平均情感分数
- **环形图**: 评分分布
- **折线图**: 各评分的MAE

### 3. 主题建模结果
- **柱状图**: 主题分布
- **表格**: 主题详情（包含关键词）

### 4. 不匹配评论分析
- **柱状图**: 不匹配程度分布
- **饼图**: 情感置信度分布
- **可搜索表格**: 不匹配评论样本（支持搜索）

### 5. 宏观经济相关性
- 显示年度情感趋势与通胀率的相关性图表

## 注意事项

1. 确保已运行分析pipeline生成output数据
2. 如果看到"Error loading data"，检查output文件夹中是否有相应的CSV文件
3. 按Ctrl+C停止服务器

