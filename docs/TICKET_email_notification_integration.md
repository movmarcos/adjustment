# Change Request — Email Notification Integration for the Adjustment App

**Requested by:** Marcos Magri (Risk — Adjustment Engine)
**Priority:** Medium (enables user notifications; no impact on existing processing)
**Change type:** New account-level object + one grant per environment. No data access involved.

---

## Summary

The Adjustment application (Streamlit on Snowflake, database `DVLP_RAPTOR_NEWADJ`,
schema `ADJUSTMENT_APP`) needs to send email notifications to its users —
e.g. "your adjustment failed", "items awaiting your approval", "COB re-open
requested". Snowflake supports this natively via an **email notification
integration** and the built-in `SYSTEM$SEND_EMAIL` function; no external
infrastructure is required.

We are requesting the integration to be created and its usage granted to the
application's owning role. Recipient control is handled by Snowflake itself:
**emails can only ever be delivered to users of this Snowflake account whose
profile email address is verified** — so no `ALLOWED_RECIPIENTS` whitelist is
needed; the app will only notify its own Snowflake users.

## Requested changes

Per environment (names below are for DEV; please apply the corresponding
role in UAT/PROD when we promote):

```sql
-- 1. Create the integration (requires ACCOUNTADMIN or CREATE INTEGRATION)
CREATE NOTIFICATION INTEGRATION ADJ_EMAIL_INT
    TYPE = EMAIL
    ENABLED = TRUE
    COMMENT = 'Email notifications for the Adjustment app (ADJUSTMENT_APP schema) — failures, approvals, sign-off requests';

-- 2. Allow the application role to use it
GRANT USAGE ON INTEGRATION ADJ_EMAIL_INT TO ROLE DVLP_RAPTOR_OWNER;
```

| Environment | Integration name | Role to grant USAGE |
|---|---|---|
| DEV  | `ADJ_EMAIL_INT` | `DVLP_RAPTOR_OWNER` |
| UAT  | `ADJ_EMAIL_INT` | *(UAT owning role)* |
| PROD | `ADJ_EMAIL_INT` | *(PROD owning role)* |

(One integration per account is sufficient if DEV/UAT/PROD share an account —
in that case a single CREATE plus one GRANT per owning role.)

## How it will be used

- The application's stored procedures (running as `DVLP_RAPTOR_OWNER`, both
  from the Streamlit app and from the existing `ADJUSTMENT_APP` serverless
  tasks) will call:
  `SYSTEM$SEND_EMAIL('ADJ_EMAIL_INT', '<recipient(s)>', '<subject>', '<body>', 'text/html')`
- Recipients are the app's own users; the app maintains its own opt-in
  preference table. Snowflake enforces that only **verified account-user
  emails** can receive mail.
- Sending is best-effort and low volume: batched per pipeline run / debounced
  per approver — expected well under typical account email limits.

## Notes for the DBA team

- Emails are sent by Snowflake from `no-reply@snowflake.net`; nothing to
  configure on the mail side beyond (if applicable) allow-listing that sender
  in the corporate mail filter.
- No `ALLOWED_RECIPIENTS` clause is requested — deliberate. Delivery is
  already restricted to verified users of this account, which is exactly the
  population the app serves. This avoids a recurring change ticket every time
  a user joins the team.
- Users who should receive notifications need a **verified email on their
  Snowflake user profile** (`ALTER USER <u> SET EMAIL = '<corporate email>'`;
  the user confirms via Snowflake's verification mail). Many users will
  already have this via SSO provisioning — we will identify any gaps and may
  follow up with a small list of `ALTER USER ... SET EMAIL` requests.
- No objects, data, or privileges of any other kind are touched by this
  request; the integration cannot read data.

## Validation (we will run after implementation)

```sql
USE ROLE DVLP_RAPTOR_OWNER;
CALL SYSTEM$SEND_EMAIL(
    'ADJ_EMAIL_INT',
    '<a verified user email>',
    'Adjustment app — notification integration test',
    'Test successful.');
-- Expected: returns TRUE and the email arrives.
```

## Rollback

```sql
DROP NOTIFICATION INTEGRATION IF EXISTS ADJ_EMAIL_INT;
```
No other cleanup required.
