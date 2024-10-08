-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE VIEW IF NOT EXISTS TENANT_VIEW(TENANT_ONLY_COL VARCHAR) AS SELECT * FROM MULTITENANT_TEST_TABLE
UPSERT INTO TENANT_VIEW (ORGANIZATION_ID, TENANT_ONLY_COL) VALUES ('defaultOrg', 'defaultData')
CREATE VIEW IF NOT EXISTS TENANT_VIEW_WITH_SALT(TENANT_ONLY_COL VARCHAR) AS SELECT * FROM MULTITENANT_TEST_TABLE_WITH_SALT
UPSERT INTO TENANT_VIEW_WITH_SALT (ORGANIZATION_ID, TENANT_ONLY_COL) VALUES ('defaultOrg', 'defaultData')
