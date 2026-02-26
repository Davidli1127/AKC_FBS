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
