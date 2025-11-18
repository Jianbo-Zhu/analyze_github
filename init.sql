-- 初始化数据库表结构

-- 项目基本信息表
-- 然后创建表（如果不存在）
CREATE TABLE IF NOT EXISTS projects (
    id INT AUTO_INCREMENT PRIMARY KEY,
    github_id BIGINT UNIQUE,
    name VARCHAR(255) NOT NULL,
    full_name VARCHAR(255) NOT NULL UNIQUE,
    owner_id BIGINT,
    description TEXT,
    created_at DATETIME,
    updated_at DATETIME,
    pushed_at DATETIME,
    stargazers_count INT,
    forks_count INT,
    open_issues_count INT,
    license_name VARCHAR(100),
    homepage VARCHAR(255),
    default_branch VARCHAR(50),
    contributors_count INT,
    main_language VARCHAR(50),
    topics VARCHAR(1024),
    created_at_timestamp BIGINT,
    updated_at_timestamp BIGINT,
    status VARCHAR(50) DEFAULT 'pending',  -- 采集状态：pending(待采集), collecting(采集ing), completed(完成), failed(失败)
    last_error TEXT  -- 错误信息，失败时存储
);

-- 项目语言分布表
CREATE TABLE IF NOT EXISTS languages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_id INT NOT NULL,
    language_name VARCHAR(50) NOT NULL,
    bytes_count BIGINT,
    percentage FLOAT,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    UNIQUE KEY unique_project_language (project_id, language_name)
);

-- 贡献者信息表
CREATE TABLE IF NOT EXISTS contributors (
    github_id BIGINT PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    avatar_url VARCHAR(255),
    html_url VARCHAR(255),
    contributions INT,
    email VARCHAR(255),
    location VARCHAR(255),
    company VARCHAR(255),
    created_at DATETIME
);

-- 项目-贡献者关联表
CREATE TABLE IF NOT EXISTS project_contributors (
    project_id INT NOT NULL,
    contributor_id BIGINT NOT NULL,
    contributions INT,
    PRIMARY KEY (project_id, contributor_id),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (contributor_id) REFERENCES contributors(github_id) ON DELETE CASCADE
);

-- 提交记录表
CREATE TABLE IF NOT EXISTS commits (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_id INT NOT NULL,
    contributor_id BIGINT,
    sha VARCHAR(40) NOT NULL,
    message TEXT,
    created_at DATETIME,
    author_name VARCHAR(255),
    author_email VARCHAR(255),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (contributor_id) REFERENCES contributors(github_id) ON DELETE SET NULL,
    UNIQUE KEY unique_commit (project_id, sha)
);

-- 项目统计数据表
CREATE TABLE IF NOT EXISTS statistics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_id INT NOT NULL UNIQUE,
    total_commits INT,
    total_issues INT,
    total_pulls INT,
    contributors_count INT,
    watchers_count INT,
    network_count INT,
    created_month VARCHAR(20),
    updated_month VARCHAR(20),
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
);

-- 项目主题标签表
CREATE TABLE IF NOT EXISTS topics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_id INT NOT NULL,
    topic_name VARCHAR(100) NOT NULL,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    UNIQUE KEY unique_project_topic (project_id, topic_name)
);

-- PR信息表
CREATE TABLE IF NOT EXISTS pull_requests (
    id INT AUTO_INCREMENT PRIMARY KEY,
    project_id INT NOT NULL,
    pr_number INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    body TEXT,
    state VARCHAR(20) NOT NULL,
    creator_id BIGINT,
    created_at DATETIME,
    updated_at DATETIME,
    closed_at DATETIME,
    merged_at DATETIME,
    merged BOOLEAN,
    commits_count INT,
    additions INT,
    deletions INT,
    changed_files INT,
    FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
    FOREIGN KEY (creator_id) REFERENCES contributors(github_id) ON DELETE SET NULL,
    UNIQUE KEY unique_pr (project_id, pr_number)
);

-- 索引优化
CREATE INDEX idx_projects_stargazers ON projects(stargazers_count);
CREATE INDEX idx_projects_forks ON projects(forks_count);
CREATE INDEX idx_projects_updated_at ON projects(updated_at);
CREATE INDEX idx_contributors_location ON contributors(location);
CREATE INDEX idx_commits_created_at ON commits(created_at);
CREATE INDEX idx_languages_language_name ON languages(language_name);
CREATE INDEX idx_topics_topic_name ON topics(topic_name);
CREATE INDEX idx_pull_requests_created_at ON pull_requests(created_at);
CREATE INDEX idx_pull_requests_state ON pull_requests(state);
CREATE INDEX idx_pull_requests_creator_id ON pull_requests(creator_id);

-- Fix the foreign key constraint definition
ALTER TABLE projects ADD CONSTRAINT fk_projects_owner_id 
FOREIGN KEY (owner_id) REFERENCES contributors(github_id) ON DELETE SET NULL;