BEGIN;

CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL
);

-- Running upgrade None -> 1d03ecf042dd

CREATE TABLE companies (
    id SERIAL NOT NULL,
    name VARCHAR(255),
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE lists (
    id SERIAL NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE locales (
    id SERIAL NOT NULL,
    locale VARCHAR(14),
    PRIMARY KEY (id),
    UNIQUE (locale)
);

CREATE TABLE countries (
    id SERIAL NOT NULL,
    code VARCHAR(2),
    name VARCHAR(100) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (code),
    UNIQUE (name)
);

CREATE TABLE campaigns (
    id SERIAL NOT NULL,
    company_id INTEGER,
    flight_date TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    end_date TIMESTAMP WITHOUT TIME ZONE,
    impression_limit INTEGER NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY(company_id) REFERENCES companies (id)
);

CREATE INDEX ix_campaigns_end_date ON campaigns (end_date);

CREATE INDEX ix_campaigns_flight_date ON campaigns (flight_date);

CREATE TABLE tiles (
    id SERIAL NOT NULL,
    campaign_id INTEGER,
    target_url VARCHAR(255) NOT NULL,
    bg_color VARCHAR(16) NOT NULL,
    title VARCHAR(255) NOT NULL,
    type VARCHAR(40) NOT NULL,
    image_uri TEXT NOT NULL,
    enhanced_image_uri TEXT,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY(campaign_id) REFERENCES campaigns (id)
);

CREATE TABLE campaigns_locales (
    locale_id INTEGER NOT NULL,
    campaign_id INTEGER NOT NULL,
    PRIMARY KEY (locale_id, campaign_id),
    FOREIGN KEY(campaign_id) REFERENCES campaigns (id),
    FOREIGN KEY(locale_id) REFERENCES locales (id)
);

CREATE TABLE campaigns_countries (
    country_id INTEGER NOT NULL,
    campaign_id INTEGER NOT NULL,
    PRIMARY KEY (country_id, campaign_id),
    FOREIGN KEY(campaign_id) REFERENCES campaigns (id),
    FOREIGN KEY(country_id) REFERENCES countries (id)
);

CREATE TABLE tiles_lists (
    tile_id INTEGER NOT NULL,
    list_id INTEGER NOT NULL,
    PRIMARY KEY (tile_id, list_id),
    FOREIGN KEY(list_id) REFERENCES lists (id),
    FOREIGN KEY(tile_id) REFERENCES tiles (id)
);

CREATE TABLE unique_counts_daily (
    id SERIAL NOT NULL,
    tile_id INTEGER,
    day DATE NOT NULL,
    impression BOOLEAN NOT NULL,
    locale VARCHAR(5) NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY(tile_id) REFERENCES tiles (id)
);

CREATE INDEX ix_unique_counts_daily_country_code ON unique_counts_daily (country_code);

CREATE INDEX ix_unique_counts_daily_day ON unique_counts_daily (day);

CREATE INDEX ix_unique_counts_daily_locale ON unique_counts_daily (locale);

CREATE TABLE impression_stats_daily (
    tile_id INTEGER NOT NULL,
    day DATE NOT NULL,
    impressions INTEGER NOT NULL,
    clicks INTEGER NOT NULL,
    pinned INTEGER NOT NULL,
    blocked INTEGER NOT NULL,
    position INTEGER NOT NULL,
    locale VARCHAR(5) NOT NULL,
    country_code VARCHAR(2) NOT NULL,
    os VARCHAR(64),
    browser VARCHAR(64),
    version VARCHAR(64),
    device VARCHAR(64)
);

CREATE INDEX ix_impression_stats_daily_browser ON impression_stats_daily (browser);

CREATE INDEX ix_impression_stats_daily_country_code ON impression_stats_daily (country_code);

CREATE INDEX ix_impression_stats_daily_day ON impression_stats_daily (day);

CREATE INDEX ix_impression_stats_daily_device ON impression_stats_daily (device);

CREATE INDEX ix_impression_stats_daily_locale ON impression_stats_daily (locale);

CREATE INDEX ix_impression_stats_daily_os ON impression_stats_daily (os);

CREATE INDEX ix_impression_stats_daily_position ON impression_stats_daily (position);

CREATE INDEX ix_impression_stats_daily_version ON impression_stats_daily (version);

CREATE TABLE unique_hlls (
    unique_counts_daily_id INTEGER,
    index SMALLSERIAL NOT NULL,
    value SMALLINT NOT NULL,
    FOREIGN KEY(unique_counts_daily_id) REFERENCES unique_counts_daily (id)
);

CREATE INDEX ix_unique_hlls_index ON unique_hlls (index);

CREATE INDEX ix_unique_hlls_value ON unique_hlls (value);

INSERT INTO alembic_version (version_num) VALUES ('1d03ecf042dd');

COMMIT;

