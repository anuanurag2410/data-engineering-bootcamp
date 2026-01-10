-- First Step 
CREATE OR REPLACE API INTEGRATION dataxbootcamprepo
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = (
        'https://github.com/anuanurag2410/'
    )
    ENABLED = TRUE
    ALLOWED_AUTHENTICATION_SECRETS = ALL
    COMMENT = 'API integration for Data Engineering Bootcamp GitHub repo';

    
-- Second STep 
ALTER GIT REPOSITORY datax_bootcamp_repo
SET AUTHENTICATION = (
    TYPE = 'GITHUB'
    TOKEN = '<PASTE_TOKEN_HERE>'
);

CREATE OR REPLACE GIT REPOSITORY datax_bootcamp_repo
    API_INTEGRATION = dataxbootcampcode
    ORIGIN = 'https://github.com/anuanurag2410/data-engineering-bootcamp.git';
