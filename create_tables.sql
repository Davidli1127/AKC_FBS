-- Admin Users Table for new login system
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'AdminUsers' AND xtype = 'U')
CREATE TABLE AdminUsers (
    admin_id         UNIQUEIDENTIFIER NOT NULL PRIMARY KEY DEFAULT NEWID(),
    username         NVARCHAR(100)    NOT NULL UNIQUE,
    password_hash    NVARCHAR(255)    NOT NULL,
    email            NVARCHAR(200)    NULL,
    created_at       DATETIME         NOT NULL DEFAULT GETDATE(),
    is_active        BIT              NOT NULL DEFAULT 1
);

-- Default admin account (password: 'akc2026')
IF NOT EXISTS (SELECT * FROM AdminUsers WHERE username = 'admin')
BEGIN
    INSERT INTO AdminUsers (username, password_hash, email, is_active)
    VALUES ('admin', '5994471abb01112afcc18159f6cc74b4f511b99806da59b3caf5a9c173cacfc5', 'admin@akc.local', 1);
END

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'FBS_Forms' AND xtype = 'U')
CREATE TABLE FBS_Forms (
    form_id      NVARCHAR(100) NOT NULL,
    form_title   NVARCHAR(300) NOT NULL,
    form_number  NVARCHAR(100) NULL,
    description  NVARCHAR(MAX) NULL,
    config_json  NVARCHAR(MAX) NULL,
    created_at   DATETIME      NOT NULL DEFAULT GETDATE(),
    updated_at   DATETIME      NOT NULL DEFAULT GETDATE(),
    is_deleted   BIT           NOT NULL DEFAULT 0,
    deleted_at   DATETIME      NULL,
    CONSTRAINT PK_FBS_Forms PRIMARY KEY (form_id)
);

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = 'FBS_Responses' AND xtype = 'U')
CREATE TABLE FBS_Responses (
    response_id      INT           IDENTITY(1,1) NOT NULL,
    form_id          NVARCHAR(100) NOT NULL,
    course_id        NVARCHAR(100) NOT NULL,
    class_code       NVARCHAR(100) NULL,
    course_title     NVARCHAR(500) NULL,
    course_date      NVARCHAR(50)  NULL,
    venue            NVARCHAR(200) NULL,
    submitted_at     DATETIME      NOT NULL DEFAULT GETDATE(),
    participant_name NVARCHAR(200) NULL,
    id_number        NVARCHAR(100) NULL,
    position_title   NVARCHAR(200) NULL,
    instructor1_name NVARCHAR(200) NULL,
    instructor2_name NVARCHAR(200) NULL,
    instructor3_name NVARCHAR(200) NULL,
    assessor1_name   NVARCHAR(200) NULL,
    assessor2_name   NVARCHAR(200) NULL,
    answers_json     NVARCHAR(MAX) NULL,
    CONSTRAINT PK_FBS_Responses PRIMARY KEY (response_id)
);

CREATE INDEX IX_FBS_Responses_form_id    ON FBS_Responses (form_id);
CREATE INDEX IX_FBS_Responses_course_id  ON FBS_Responses (course_id);
CREATE INDEX IX_FBS_Responses_submitted  ON FBS_Responses (submitted_at);
CREATE INDEX IX_FBS_Responses_id_number  ON FBS_Responses (id_number);
CREATE TABLE Feedback_Form1 (
    id INT IDENTITY(1,1) PRIMARY KEY,
    submission_time DATETIME DEFAULT GETDATE(),
    
    course_id VARCHAR(50),
    course_title NVARCHAR(255),
    course_date VARCHAR(50),
    classroom NVARCHAR(255),
    language NVARCHAR(50),
    
    instructor1_name NVARCHAR(255),
    instructor2_name NVARCHAR(255),
    instructor3_name NVARCHAR(255),
    
    A1 INT,  
    A2 INT, 
    A3 INT, 
    A4 INT, 
    A5 INT,  
    
    B1_1 INT, 
    B1_2 INT, 
    B1_3 INT, 
    B1_4 INT, 
    B1_5 INT, 
    B1_6 INT, 
    
    B2_1 INT,
    B2_2 INT,
    B2_3 INT,
    B2_4 INT,
    B2_5 INT,
    B2_6 INT,
    
    B3_1 INT,
    B3_2 INT,
    B3_3 INT,
    B3_4 INT,
    B3_5 INT,
    B3_6 INT,
    
    C1 INT,  
    C2 INT,  
    
    D INT,   
    E INT, 
    
    E1 NVARCHAR(MAX),  
    E2 NVARCHAR(MAX), 
    F NVARCHAR(MAX),   
    G NVARCHAR(MAX),   
    H NVARCHAR(MAX)  
);

CREATE INDEX IX_Feedback_Form1_course_id ON Feedback_Form1(course_id);
CREATE INDEX IX_Feedback_Form1_course_date ON Feedback_Form1(course_date);


CREATE TABLE Feedback_Form2 (
    id INT IDENTITY(1,1) PRIMARY KEY,
    submission_time DATETIME DEFAULT GETDATE(),
    
    course_id VARCHAR(50),
    course_title NVARCHAR(255),
    course_date VARCHAR(50),
    classroom NVARCHAR(255),
    language NVARCHAR(50),
    
    assessor1_name NVARCHAR(255),
    assessor2_name NVARCHAR(255),
    
    A1_1 INT, 
    A1_2 INT, 
    A1_3 INT,  
    A1_4 INT,  
    A1_5 INT, 
    
    A2_1 INT,
    A2_2 INT,
    A2_3 INT,
    A2_4 INT,
    A2_5 INT,
    
    B NVARCHAR(MAX) 
);

CREATE INDEX IX_Feedback_Form2_course_id ON Feedback_Form2(course_id);
CREATE INDEX IX_Feedback_Form2_course_date ON Feedback_Form2(course_date);
