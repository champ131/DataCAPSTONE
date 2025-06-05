
-- Create database
CREATE DATABASE IF NOT EXISTS credit_card_system;
USE credit_card_system;

-- Drop tables if they exist (comment these out if you don't want to drop existing tables)
-- DROP TABLE IF EXISTS CDW_SAPP_CREDIT_CARD;
-- DROP TABLE IF EXISTS CDW_SAPP_CUSTOMER;
-- DROP TABLE IF EXISTS cdw_sapp_branch;

-- Create the customer table
CREATE TABLE IF NOT EXISTS CDW_SAPP_CUSTOMER (
    SSN INT PRIMARY KEY,
    FIRST_NAME VARCHAR(50) NOT NULL,
    MIDDLE_NAME VARCHAR(50),
    LAST_NAME VARCHAR(50) NOT NULL,
    Credit_card_no VARCHAR(16) NOT NULL,
    FULL_STREET_ADDRESS VARCHAR(100) NOT NULL,
    CUST_CITY VARCHAR(50) NOT NULL,
    CUST_STATE VARCHAR(2) NOT NULL,
    CUST_COUNTRY VARCHAR(50) NOT NULL,
    CUST_ZIP VARCHAR(10) NOT NULL,
    CUST_PHONE VARCHAR(15),
    CUST_EMAIL VARCHAR(100),
    LAST_UPDATED VARCHAR(50),
    INDEX idx_credit_card_no (Credit_card_no)
);

-- Create the branch table
CREATE TABLE IF NOT EXISTS cdw_sapp_branch (
    BRANCH_CODE INT PRIMARY KEY,
    BRANCH_NAME VARCHAR(100) NOT NULL,
    BRANCH_STREET VARCHAR(100) NOT NULL,
    BRANCH_CITY VARCHAR(50) NOT NULL,
    BRANCH_STATE VARCHAR(2) NOT NULL,
    BRANCH_ZIP VARCHAR(10) DEFAULT '999999',
    BRANCH_PHONE VARCHAR(15),
    LAST_UPDATED VARCHAR(50),
    INDEX idx_branch_location (BRANCH_STATE, BRANCH_CITY)
);

-- Create the credit card transaction table
-- Note: Foreign key constraints are commented out to make data loading easier
CREATE TABLE IF NOT EXISTS CDW_SAPP_CREDIT_CARD (
    TRANSACTION_ID INT AUTO_INCREMENT PRIMARY KEY,
    CUST_CC_NO VARCHAR(16) NOT NULL,
    TIMEID VARCHAR(8) NOT NULL,
    CUST_SSN INT NOT NULL,
    BRANCH_CODE INT NOT NULL,
    TRANSACTION_TYPE VARCHAR(50) NOT NULL,
    TRANSACTION_VALUE DOUBLE NOT NULL,
    INDEX idx_cust_ssn (CUST_SSN),
    INDEX idx_branch_code (BRANCH_CODE),
    INDEX idx_timeid (TIMEID),
    INDEX idx_cust_cc_no (CUST_CC_NO)
    -- Uncomment these lines to add foreign key constraints
    -- , CONSTRAINT fk_credit_card_customer FOREIGN KEY (CUST_SSN) REFERENCES CDW_SAPP_CUSTOMER(SSN)
    -- , CONSTRAINT fk_credit_card_branch FOREIGN KEY (BRANCH_CODE) REFERENCES cdw_sapp_branch(BRANCH_CODE)
);

-- SQL to add foreign key constraints after data import
-- Run these commands after importing all your data:
/*
ALTER TABLE CDW_SAPP_CREDIT_CARD 
ADD CONSTRAINT fk_credit_card_customer 
FOREIGN KEY (CUST_SSN) REFERENCES CDW_SAPP_CUSTOMER(SSN);

ALTER TABLE CDW_SAPP_CREDIT_CARD 
ADD CONSTRAINT fk_credit_card_branch 
FOREIGN KEY (BRANCH_CODE) REFERENCES cdw_sapp_branch(BRANCH_CODE);
*/
