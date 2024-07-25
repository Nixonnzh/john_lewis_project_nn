


CREATE TABLE countries(
	country_id int NOT NULL,
	Country_name varchar(25) NOT NULL,
 CONSTRAINT PK_countries PRIMARY KEY (country_id )
) 
;
CREATE TABLE Statuses(
	status int NOT NULL,
	status_name varchar(25) NOT NULL,
 CONSTRAINT PK_Statuses PRIMARY KEY (status)
)
;

CREATE TABLE date_dim(
	dweek int NOT NULL,
	dmonth int NOT NULL,
	DQUARTERJL int NOT NULL,
 CONSTRAINT date_dim_pk PRIMARY KEY  (	dweek ,	dmonth ,	DQUARTERJL )
) 
;

CREATE TABLE label_dom(
	label_code nvarchar(128) NOT NULL,
	label_ques nvarchar(4000) NOT NULL,
	orig_label varchar(4000) NULL,
	Multi_cols varchar(1) NULL,
 CONSTRAINT PK__label_di__BC61C178E8262450 PRIMARY KEY (	label_code )
) 
;

CREATE TABLE label_codes(
	label_code nvarchar(128) NOT NULL,
	response_no int NOT NULL,
	text varchar(500) NULL,
 CONSTRAINT label_code_pk PRIMARY KEY (	label_code ,	response_no )
)
;

ALTER TABLE label_codes  ADD  CONSTRAINT FK_label_codes_label_dim FOREIGN KEY(label_code)
REFERENCES label_dom (label_code)
;


CREATE TABLE dwave(
	dwave int NOT NULL,
	status int NOT NULL,
	country int NOT NULL,
 CONSTRAINT dwave_pk PRIMARY KEY  (	dwave )
)
;

ALTER TABLE dwave ADD  CONSTRAINT FK_dwave_countries FOREIGN KEY(country)
REFERENCES countries (country_id)
;


ALTER TABLE dwave  ADD  CONSTRAINT FK_dwave_Statuses FOREIGN KEY(status)
REFERENCES Statuses (status)
;


CREATE TABLE dwave_rotation(
	dwave int NOT NULL,
	dweek int NOT NULL,
	dmonth int NOT NULL,
	DQUARTERJL int NOT NULL,
	DSPLITSECTION_1 int NULL,
	DSPLITSECTION_2 int NULL,
	DSPLITSECTION_3 int NULL,
	DSPLITSECTION_4 int NULL,
	DSPLITSECTION_5 int NULL,
	DSPLITSECTION_6 int NULL,
 CONSTRAINT dwave_rota_pk PRIMARY KEY  (	dwave ,	dweek ,	dmonth )
) 
;

ALTER TABLE dwave_rotation ADD  CONSTRAINT FK_dwave_rotation_date_dim FOREIGN KEY(dweek, dmonth, DQUARTERJL)
REFERENCES date_dim (dweek, dmonth, DQUARTERJL)
;



ALTER TABLE dwave_rotation   ADD  CONSTRAINT FK_dwave_rotation_dwave FOREIGN KEY(dwave)
REFERENCES dwave (dwave)
;



CREATE TABLE response_main(
	response_id int identity NOT NULL,
	dwave int NOT NULL,
	dweek int NOT NULL,
	dmonth int NOT NULL,
	S2_age int NULL,
	FC10_family_members int NULL,
	WEIGHT decimal(10, 2) NULL,
 CONSTRAINT PK_survey PRIMARY KEY  (	response_id )
) 
;

ALTER TABLE response_main  ADD  CONSTRAINT FK_surveydwave_rotation FOREIGN KEY(dwave, dweek, dmonth)
REFERENCES dwave_rotation (dwave, dweek, dmonth)
;

CREATE TABLE response_single(
	response_id int NOT NULL,
	label_code nvarchar(128) NOT NULL,
	response_no int NOT NULL,
 CONSTRAINT PK_survey_single PRIMARY KEY  (	response_id ,	label_code )
) 
;
ALTER TABLE response_single  ADD  CONSTRAINT FK_response_single_label_codes FOREIGN KEY(label_code, response_no)
REFERENCES label_codes (label_code, response_no)
;


ALTER TABLE response_single  ADD  CONSTRAINT FK_response_single_response_main FOREIGN KEY(response_id)
REFERENCES response_main (response_id)
;



CREATE TABLE response_multi(
	response_id int NOT NULL,
	label_code nvarchar(128) NOT NULL,
	response_no int NOT NULL,
 CONSTRAINT PK_survey_multi PRIMARY KEY  (	response_id ,	label_code ,	response_no )
) 
;

ALTER TABLE response_multi   ADD  CONSTRAINT FK_response_multi_label_codes FOREIGN KEY(label_code, response_no)
REFERENCES label_codes (label_code, response_no)
;



ALTER TABLE response_multi  ADD  CONSTRAINT FK_response_multi_response_main FOREIGN KEY(response_id)
REFERENCES response_main (response_id)
;
