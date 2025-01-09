CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS dvde_rep_fraud (
    report_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    event_dt TIMESTAMP NOT NULL,
    passport VARCHAR(15) NOT NULL,
    fio VARCHAR(100) NOT NULL,
    phone VARCHAR(16) NOT NULL,
    event_type TEXT NOT NULL,
    report_dt DATE NOT NULL
);