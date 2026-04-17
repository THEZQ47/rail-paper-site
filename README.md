# 轨道交通文献自动推送 (Railway Literature Auto-Push)

自动抓取 Elsevier/Scopus 数据库中最新的轨道交通领域前沿论文，并通过自动化工作流生成静态网页进行展示。
注：这是我的第一个GitHub项目，全程在Codex和Gemini的帮助下完成，希望这是好的开始

## 核心功能
* **自动化抓取**：利用 GitHub Actions 定时触发 Python 脚本，自动调用 Scopus API 检索最新文献。
* **零成本托管**：网页纯静态化，结合 Netlify 实现自动部署与全球加速访问。
* **每日更新**：无需人工干预，确保每天获取最新的学术研究动态。

## 技术栈
* **数据获取**：Python (`requests`, `json`)
* **自动化流**：GitHub Actions
* **网页展示**：HTML / CSS / JavaScript
* **部署平台**：Netlify

## 快速开始

如果你想复刻这个项目（Fork），请按照以下步骤进行配置：

### 1. 申请 API 密钥
前往 [Elsevier Developer Portal](https://dev.elsevier.com/) 注册账号并申请一个 `API Key`。

### 2. 配置 GitHub Secrets
在你的 GitHub 仓库中，进入 `Settings` -> `Secrets and variables` -> `Actions`，添加以下机密信息：
* `SCOPUS_API_KEY`：填入你刚刚申请的 API Key（必填项）。

### 3. 部署到 Netlify
* 登录 Netlify，选择 `Import an existing project`。
* 关联你的 GitHub 账号，选择本仓库。
* 直接点击 `Deploy site` 即可完成部署（无需复杂的构建命令）。

## 目录结构
```text
├── .github/workflows/   # GitHub Actions 自动化脚本
├── src/                 # Python 爬虫核心代码
├── web/                 # 前端展示网页 (HTML/CSS)
└── README.md            # 项目说明文档