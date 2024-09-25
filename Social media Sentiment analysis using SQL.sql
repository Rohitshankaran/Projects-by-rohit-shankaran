-- Step 1: Create the Database
CREATE DATABASE SocialMediaDB;

-- Step 2: Use the Database
USE SocialMediaDB;

-- Step 3: Create the Users Table
CREATE TABLE Users (
    user_id INT PRIMARY KEY,
    username VARCHAR(100),
    age INT,
    gender VARCHAR(10),
    country VARCHAR(50)
);

-- Step 4: Create the Posts Table
CREATE TABLE Posts (
    post_id INT PRIMARY KEY,
    user_id INT,
    content TEXT,
    post_date DATE,
    likes INT,
    shares INT,
    comments INT,
    sentiment_score DECIMAL(3, 2), -- Scale from -1 (negative) to 1 (positive)
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);

-- Step 5: Insert Sample Data into Users Table
INSERT INTO Users (user_id, username, age, gender, country) VALUES
(1, 'Alice', 30, 'Female', 'USA'),
(2, 'Bob', 25, 'Male', 'Canada'),
(3, 'Charlie', 35, 'Male', 'UK'),
(4, 'Diana', 28, 'Female', 'Australia'),
(5, 'Evan', 22, 'Male', 'USA');

-- Step 6: Insert Sample Data into Posts Table
INSERT INTO Posts (post_id, user_id, content, post_date, likes, shares, comments, sentiment_score) VALUES
(1, 1, 'Loving the new features of the app!', '2023-01-15', 50, 5, 10, 0.8),
(2, 2, 'Not happy with the recent update.', '2023-01-20', 30, 3, 1, -0.6),
(3, 3, 'Great day for a hike!', '2023-01-22', 40, 70, 15, 0.9),
(4, 4, 'Could be better, facing some issues.', '2023-01-25', 20, 2, 3, -0.2),
(5, 5, 'Had an amazing time at the concert!', '2023-01-30', 80, 10, 5, 0.7);

-- Step 7: Analyze Overall Sentiment
SELECT 
    AVG(sentiment_score) AS average_sentiment,
    COUNT(post_id) AS total_posts,
    SUM(likes) AS total_likes,
    SUM(shares) AS total_shares,
    SUM(comments) AS total_comments
FROM 
    Posts;

-- Step 8: Sentiment Analysis by User
SELECT 
    u.username,
    AVG(p.sentiment_score) AS avg_sentiment,
    COUNT(p.post_id) AS post_count,
    SUM(p.likes) AS total_likes
FROM 
    Users u
LEFT JOIN 
    Posts p ON u.user_id = p.user_id
GROUP BY 
    u.username
ORDER BY 
    avg_sentiment DESC;

-- Step 9: Posts with Highest Engagement
SELECT 
    p.post_id,
    u.username,
    p.content,
    p.likes,
    p.shares,
    p.comments,
    p.sentiment_score
FROM 
    Posts p
JOIN 
    Users u ON p.user_id = u.user_id
ORDER BY 
    (p.likes + p.shares + p.comments) DESC
LIMIT 5;

-- Step 10: Sentiment Distribution
SELECT 
    CASE 
        WHEN sentiment_score < -0.5 THEN 'Negative'
        WHEN sentiment_score BETWEEN -0.5 AND 0.5 THEN 'Neutral'
        ELSE 'Positive'
    END AS sentiment_category,
    COUNT(post_id) AS post_count
FROM 
    Posts
GROUP BY 
    sentiment_category;


