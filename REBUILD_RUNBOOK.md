# Zapbot Droplet Rebuild Runbook

Rebuild of the compromised Divento Zapbot droplet onto a fresh, hardened box.
Written for someone comfortable with Python but new to Laravel/PHP/sysadmin — the
PHP/Apache parts are explained inline. Work top to bottom; do not skip Phase 0.

**🔒 = SECURITY-CRITICAL.** These steps directly close the vector that got the
box owned (unrestricted Filament/Livewire file upload → PHP webshell as www-data,
on an internet-exposed admin running in debug mode with PHP-executable upload
dirs). Skipping any 🔒 step risks a third compromise.

---

## Things you must provide / decide before starting

| Placeholder | Meaning | Example |
|---|---|---|
| `DOMAIN` | The real domain for the admin (client wants no "numerals" URL) | `zapbot.diventoapp.com` |
| `MYIP` | Your home/office IP for SSH allow-listing (`curl ifconfig.me`) | `95.104.27.36` |
| `ADMIN_EMAILS` | Emails allowed into the admin via Cloudflare Access | you + Fiona |
| OS | Ubuntu 24.04 LTS (matches old box's Apache 2.4.62 lineage) | — |
| Droplet size | Same as before: 2 vCPU / 4 GB (legit workload fits; only the miner made it look small) | — |

Prereq: the `DOMAIN` must be on a **Cloudflare** account (free tier is fine) —
Cloudflare Access is how we lock the admin. If it isn't yet, do that first:
add the site in Cloudflare, switch the registrar's nameservers to Cloudflare's,
wait for "Active".

---

## Phase 0 — Local prep & secret rotation (before any droplet exists)

🔒 **Rotate every secret the old box held. Treat all old values as burned.**

| Secret | Action | Status |
|---|---|---|
| OpenAI API key | Already rotated 2026-05-15 | ✅ |
| Google Places key (`GOOGLE_PLACES_KEY`) | Create new key in Google Cloud console, restrict it to the Places API + (ideally) the droplet's egress IP; delete the old key | ☐ |
| MySQL app password | Invent a new strong one now; used in Phase 4/5 | ☐ |
| Laravel `APP_KEY` | Generated fresh on the box in Phase 5 (`php artisan key:generate`) | ☐ |
| Filament admin login | New email/password, set in Phase 5 | ☐ |
| DigitalOcean API tokens | Revoke all existing tokens; enable account 2FA | ☐ |
| SSH keypair | Generate a NEW dedicated key, do **not** reuse the old box's key | ☐ |

Generate the new SSH key locally:
```sh
ssh-keygen -t ed25519 -a 100 -f ~/.ssh/zapbot_new -C "zapbot-rebuild-2026"
```

Sanity-check the deploy sources on your Mac (do NOT trust anything pulled from
the old box without this):
```sh
cd "/Users/emilshalamberidze/Desktop/divento-scrapper 2"
# Confirm the 3 fixes are present in the audited Filament copy:
grep -n "job_type='permanent' AND status='new'" untrusted-from-compromised-box/code/diventoscrapper/scripts/scrape_destinations.py
grep -n "_recover_stale" untrusted-from-compromised-box/code/diventoscrapper/scripts/scrape_temporary_destinations.py
grep -n "default('permanent')" untrusted-from-compromised-box/code/diventoscrapper/app/Filament/Resources/ScrapperResource.php   # expect: no output
# Confirm no stray shells remain in the web root we'll deploy:
find untrusted-from-compromised-box/code/diventoscrapper/public -name '*.php' | grep -v '^.*public/index.php$'   # expect only index.php
```
We will **not** ship the pulled `vendor/`, `.env`, `storage/` runtime, or
`node_modules/` — those get rebuilt clean on the box.

---

## Phase 1 — Provision the droplet

1. DigitalOcean → Create Droplet → Ubuntu 24.04 LTS, 2 vCPU / 4 GB, your region,
   **SSH key = the new `zapbot_new.pub`**, no password auth.
2. 🔒 **DO Cloud Firewall** (Networking → Firewalls → Create). It sits in front
   of the droplet so a host misconfig can't bypass it. Inbound rules:
   - SSH `TCP 22` — source: **`MYIP` only**
   - HTTP `TCP 80` — source: **Cloudflare IP ranges** (cloudflare.com/ips) — used only for the TLS challenge + redirect
   - HTTPS `TCP 443` — source: **Cloudflare IP ranges only**
   - Outbound: allow all (the scraper needs to reach OpenAI/Google)

   Restricting 80/443 to Cloudflare IPs means nobody can hit the origin IP
   directly to bypass Cloudflare Access. Attach the firewall to the droplet.
3. First contact:
   ```sh
   ssh -i ~/.ssh/zapbot_new root@NEW_DROPLET_IP
   ```

---

## Phase 2 — Base hardening (before the app is installed)

```sh
# create a non-root sudo user you'll use from now on
adduser deploy
usermod -aG sudo deploy
rsync --archive --chown=deploy:deploy ~/.ssh /home/deploy

apt update && apt -y full-upgrade

# automatic security patches
apt -y install unattended-upgrades
dpkg-reconfigure -plow unattended-upgrades   # choose "Yes"

# fail2ban (brute-force throttling; the old box saw 71k SSH attempts)
apt -y install fail2ban
systemctl enable --now fail2ban

# swap (4GB box + a heavy scrape; prevents OOM kills)
fallocate -l 4G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab
```

🔒 SSH hardening — edit `/etc/ssh/sshd_config`, set:
```
PermitRootLogin no
PasswordAuthentication no
PubkeyAuthentication yes
PermitEmptyPasswords no
```
Then:
```sh
systemctl restart ssh
```
**Open a second terminal and confirm `ssh -i ~/.ssh/zapbot_new deploy@IP` works
before closing your root session.** From here on, log in as `deploy`.

🔒 host firewall (defence-in-depth mirror of the DO firewall):
```sh
sudo apt -y install ufw
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow from MYIP to any port 22 proto tcp
sudo ufw allow 80,443/tcp
sudo ufw enable
```

---

## Phase 3 — Install the stack

```sh
sudo apt -y install apache2 mysql-server \
  php php-cli php-mysql php-mbstring php-xml php-curl php-zip php-gd php-bcmath php-intl libapache2-mod-php \
  python3 python3-venv python3-pip \
  redis-server \
  unzip git curl

# Ubuntu 24.04 ships PHP 8.3 — the admin needs >= 8.2 (Laravel 12 / Filament 3.3
# / Livewire 3.6). Confirm you did NOT get 8.1:
php -v        # expect 8.2 or 8.3

# 🔒 Redis is REQUIRED — the FastAPI temp-scraper uses a Celery+Redis task
# queue. Lock it to localhost (the relayed advice's "open Redis" warning):
sudo sed -i 's/^# *bind .*/bind 127.0.0.1 ::1/' /etc/redis/redis.conf
sudo sed -i 's/^# *requirepass .*/requirepass CHOOSE_A_REDIS_PASSWORD/' /etc/redis/redis.conf
sudo systemctl enable --now redis-server
sudo systemctl restart redis-server
redis-cli -a CHOOSE_A_REDIS_PASSWORD ping   # expect PONG

# Composer (PHP's pip) — installs PHP dependencies
cd /tmp && curl -sS https://getcomposer.org/installer | php
sudo mv composer.phar /usr/local/bin/composer

# Node (only to build the admin's CSS/JS assets once)
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt -y install nodejs

sudo a2enmod php* rewrite headers ssl
```

🔒 Create a dedicated app user so neither Apache nor the scrapers run as a
broadly-privileged account, and the blast radius of any future web compromise
is just this app:
```sh
sudo adduser --system --group --home /var/www/zapbot zapbot
sudo usermod -aG zapbot www-data     # Apache can read app files via group
```

---

## Phase 4 — MySQL + data restore

```sh
sudo mysql_secure_installation        # set a root password, answer "Y" to all

sudo mysql -u root -p <<'SQL'
CREATE DATABASE diventoscrapper CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'zapbot'@'127.0.0.1' IDENTIFIED BY 'THE_NEW_MYSQL_PASSWORD';
GRANT ALL PRIVILEGES ON diventoscrapper.* TO 'zapbot'@'127.0.0.1';
FLUSH PRIVILEGES;
SQL
```

🔒 Ensure MySQL is localhost-only (it was, on the old box — keep it that way):
in `/etc/mysql/mysql.conf.d/mysqld.cnf` confirm `bind-address = 127.0.0.1`,
then `sudo systemctl restart mysql`.

Copy the dumps up from your Mac and import (schema first, then data):
```sh
# from your Mac:
scp -i ~/.ssh/zapbot_new "untrusted-from-compromised-box/db/schema.sql" "untrusted-from-compromised-box/db/data.sql" deploy@IP:/tmp/

# on the droplet:
sudo mysql -u root -p diventoscrapper < /tmp/schema.sql
sudo mysql -u root -p diventoscrapper < /tmp/data.sql
rm /tmp/schema.sql /tmp/data.sql
```
The data dump is the Barcelona/Lisbon-class stuck rows included — the stale-job
recovery we added will clean those up automatically on first cron run.

---

## Phase 5 — Deploy the Laravel/Filament admin

> **What this is:** the admin is a PHP web app. `composer install` fetches its
> PHP libraries (like `pip install -r`). `.env` holds its config/secrets (same
> idea as a Python `.env`). `php artisan …` are its management commands. You
> write no PHP.

```sh
# from your Mac — ship code WITHOUT vendor/.env/storage-runtime/node_modules:
rsync -av --delete \
  --exclude vendor --exclude node_modules --exclude .env \
  --exclude 'storage/app/private/livewire-tmp' \
  --exclude 'storage/logs/*' --exclude 'storage/framework/cache/*' \
  --exclude 'storage/framework/views/*' --exclude 'storage/framework/sessions/*' \
  -e "ssh -i ~/.ssh/zapbot_new" \
  "untrusted-from-compromised-box/code/diventoscrapper/" deploy@IP:/var/www/zapbot/
```

On the droplet:
```sh
cd /var/www/zapbot
composer install --no-dev --optimize-autoloader      # rebuild PHP deps clean
composer audit                                       # 🔒 fail/patch on known CVEs
npm ci && npm run build                              # build admin assets
```

🔒 Write `/var/www/zapbot/.env` — **production, debug OFF** (debug-on in prod was
a contributing factor). Minimum keys:
```
APP_NAME=Zapbot
APP_ENV=production
APP_KEY=
APP_DEBUG=false
APP_URL=https://DOMAIN

DB_CONNECTION=mysql
DB_HOST=127.0.0.1
DB_PORT=3306
DB_DATABASE=diventoscrapper
DB_USERNAME=zapbot
DB_PASSWORD=THE_NEW_MYSQL_PASSWORD

OPENAI_API_KEY=the-new-openai-key
GOOGLE_PLACES_KEY=the-new-places-key
TEMP_SCRAPER_API_URL=http://127.0.0.1:8000

LOG_LEVEL=warning
SESSION_DRIVER=database
CACHE_STORE=database
```
Then:
```sh
php artisan key:generate            # fills APP_KEY
php artisan storage:link
php artisan migrate --force         # reconciles any schema delta vs the dump; safe/idempotent
php artisan config:cache route:cache view:cache

# create the admin login with the NEW credentials
php artisan make:filament-user      # enter new email + strong password

# 🔒 ownership/permissions: app owned by zapbot, group-readable by www-data,
# only storage + cache writable
sudo chown -R zapbot:zapbot /var/www/zapbot
sudo find /var/www/zapbot -type d -exec chmod 750 {} \;
sudo find /var/www/zapbot -type f -exec chmod 640 {} \;
sudo chmod -R 770 /var/www/zapbot/storage /var/www/zapbot/bootstrap/cache
```

🔒 Apache vhost — create `/etc/apache2/sites-available/zapbot.conf`. Note the
**hard block on PHP execution anywhere except the front controller**; this is
the single change that would have neutralised the uploaded `img.php`:
```apache
<VirtualHost *:80>
    ServerName DOMAIN
    DocumentRoot /var/www/zapbot/public

    <Directory /var/www/zapbot/public>
        AllowOverride All
        Require all granted
    </Directory>

    # 🔒 No PHP may execute under storage/ or any upload/temp dir.
    # A dropped .php there becomes an inert text file.
    <DirectoryMatch "^/var/www/zapbot/(storage|bootstrap/cache)">
        php_admin_flag engine off
        <FilesMatch "\.php$">
            Require all denied
        </FilesMatch>
    </DirectoryMatch>

    # 🔒 Never serve dotfiles / .env / vcs
    <FilesMatch "^\.">
        Require all denied
    </FilesMatch>

    Header always set X-Content-Type-Options "nosniff"
    Header always set X-Frame-Options "SAMEORIGIN"
    Header always set Referrer-Policy "strict-origin-when-cross-origin"

    ErrorLog  ${APACHE_LOG_DIR}/zapbot_error.log
    CustomLog ${APACHE_LOG_DIR}/zapbot_access.log combined
</VirtualHost>
```
```sh
sudo a2dissite 000-default
sudo a2ensite zapbot
sudo systemctl reload apache2
```

🔒 **Lock down the Filament file-upload that was the entry point.** In every
Filament `FileUpload` field in `app/Filament/Resources/…`, ensure there is an
explicit allow-list and size cap, and storage is private, e.g.:
```php
FileUpload::make('whatever')
    ->acceptedFileTypes(['image/png','image/jpeg'])   // never allow php/*, */*
    ->maxSize(5120)
    ->disk('local')->directory('uploads')->visibility('private')
```
If the app has **no** legitimate upload field, even better — but the global
Livewire upload route still exists, so the Apache PHP-exec block above plus
Cloudflare Access (Phase 7) are what truly close it.

---

## Phase 6 — Deploy the Python scrapers

🔒 **FastAPI `.env`** at `/opt/divento-temp/.env`. Only `OPENAI_API_KEY` is a
real secret; the model lines are NOT optional — omitting them silently
downgrades the scraper to weaker default models (`gpt-5-mini`/`gpt-5-nano`) and
changes client output. Source of truth = `app/config.py` (pydantic-settings,
`extra="ignore"` so unknown keys are dead). Do NOT copy the old root `.env`'s
`DB_URI`/`REDIS_URL`/`SECRET_KEY`/`TEMP_TRANSLATION_WORKERS` etc. — the current
app ignores them.
```ini
OPENAI_API_KEY=__FILL__            # new rotated key
OPENAI_TEMP_MODEL=gpt-5.2
OPENAI_TEMP_SEARCH_MODEL=gpt-5.2
OPENAI_TEMP_COPY_MODEL=gpt-5.2
OPENAI_TEMP_TRANSLATION_MODEL=gpt-5.2
RESULT_DIR=./data
LOG_DIR=./logs
LOG_LEVEL=INFO
TEMP_MAX_CITIES=0
TEMP_MAX_EXHIBITIONS=0
```
> Open item: the old root `.env` carried `REDIS_URL` with a Docker `redis`
> hostname even though `config.py` ignores it — suggests Celery/Redis may be
> used outside pydantic Settings. Verify empirically at the end of this phase
> (start service → `/api/healthz` → run a test job); keep `redis-server`
> installed regardless (the admin needs it anyway).

**FastAPI temporary-scraper** (from the clean Git worktree, not the pulled copy):
```sh
# from your Mac:
rsync -av --exclude .venv --exclude data --exclude __pycache__ \
  -e "ssh -i ~/.ssh/zapbot_new" \
  "/Users/emilshalamberidze/Desktop/divento-scrapper 2/.claude/worktrees/keen-hamilton-2a0db9/" \
  deploy@IP:/opt/divento-temp/

# on the droplet:
cd /opt/divento-temp
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
pip install pip-audit && pip-audit          # 🔒 dependency CVE check

# REQUIRED: this scraper drives a headless browser via Playwright. pip installs
# the library but NOT the browser or its OS libraries — do both, or the scraper
# silently produces nothing:
sudo /opt/divento-temp/.venv/bin/playwright install-deps      # OS libs (apt)
# install the browser into a fixed path the service user can read:
sudo mkdir -p /opt/divento-temp/.pw-browsers
sudo PLAYWRIGHT_BROWSERS_PATH=/opt/divento-temp/.pw-browsers \
  /opt/divento-temp/.venv/bin/playwright install chromium    # ~400MB

# REQUIRED: NLTK needs its corpora downloaded once (used for text processing).
python - <<'PY'
import nltk
for pkg in ("punkt", "punkt_tab", "stopwords", "wordnet"):
    try: nltk.download(pkg, quiet=True)
    except Exception as e: print("nltk", pkg, "->", e)
print("nltk data ready")
PY

deactivate
sudo chown -R zapbot:zapbot /opt/divento-temp
```
> Note: Playwright's browser is downloaded into the venv user's home cache. Since
> the service runs as `zapbot`, run `playwright install chromium` **as the
> `zapbot` user** (e.g. `sudo -u zapbot ... playwright install chromium`) or set
> `PLAYWRIGHT_BROWSERS_PATH=/opt/divento-temp/.pw-browsers` consistently in both
> the install command and the systemd unit so the service can find it.
Create `/etc/systemd/system/divento-temp.service` (binds **127.0.0.1 only** —
never public):
```ini
[Unit]
Description=Divento temp scraper (FastAPI)
After=network.target mysql.service redis-server.service

[Service]
User=zapbot
WorkingDirectory=/opt/divento-temp
Environment=PYTHONPATH=/opt/divento-temp
Environment=PLAYWRIGHT_BROWSERS_PATH=/opt/divento-temp/.pw-browsers
EnvironmentFile=/opt/divento-temp/.env
ExecStart=/opt/divento-temp/.venv/bin/python -m uvicorn app.ui:app --host 127.0.0.1 --port 8000
Restart=on-failure

[Install]
WantedBy=multi-user.target
```
```sh
sudo systemctl daemon-reload && sudo systemctl enable --now divento-temp
curl -s http://127.0.0.1:8000/api/healthz   # expect ok
```

**The two cron scrapers** live in the Filament tree (`scripts/`), already
carrying our 3 fixes. Build their venv and schedule them as the `zapbot` user
(NOT root, NOT www-data — old box ran them as root):
```sh
cd /var/www/zapbot/scripts
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt 2>/dev/null \
  || ./venv/bin/pip install pymysql requests python-dotenv openai
sudo crontab -u zapbot -e
```
Add:
```cron
STALE_JOB_TIMEOUT_MINUTES=360
* * * * * /var/www/zapbot/scripts/venv/bin/python3 /var/www/zapbot/scripts/scrape_destinations.py >> /var/www/zapbot/scripts/scraper_cron.log 2>&1
* * * * * flock -n /tmp/divento_temp.lock /var/www/zapbot/scripts/venv/bin/python3 /var/www/zapbot/scripts/scrape_temporary_destinations.py >> /var/www/zapbot/scripts/scraper_temp_cron.log 2>&1
```

---

## Phase 7 — HTTPS + Cloudflare Access 🔒

Sequencing matters (cert before proxy):

1. In Cloudflare DNS, create an **A record** `DOMAIN → NEW_DROPLET_IP`, initially
   **DNS-only (grey cloud)** so certbot can validate directly.
2. Issue a real cert on the origin:
   ```sh
   sudo apt -y install certbot python3-certbot-apache
   sudo certbot --apache -d DOMAIN --redirect --hsts
   ```
   This also rewrites the vhost to force 80→443. Re-confirm the 🔒
   `DirectoryMatch` PHP-exec block survived the rewrite (certbot edits the file).
3. In Cloudflare, switch the A record to **Proxied (orange cloud)**, SSL/TLS
   mode **Full (strict)**.
4. 🔒 **Cloudflare Access** (Zero Trust → Access → Applications): create a
   self-hosted application covering `DOMAIN` (the whole site is the admin).
   Policy: **Allow** → emails in `ADMIN_EMAILS`; everything else denied. Now the
   login page, the Livewire upload route, everything — is unreachable to the
   public internet. This is the hard close on the original vector.
5. Add a Cloudflare WAF managed ruleset (free tier) for defence in depth.

---

## Phase 8 — Verification & cutover

Run all of these before pointing the client at it:

- [ ] `https://DOMAIN` → Cloudflare Access login appears (not the app directly)
- [ ] After Access auth, Filament admin loads, you can log in with the new creds
- [ ] 🔒 `curl https://DOMAIN/storage/test.php` style probe → PHP not executed
- [ ] Create a **permanent** job → only `scrape_destinations.py` picks it (filename has no `temp_`); create a **temporary** job → only the temp pipeline picks it. (Validates fix #1.)
- [ ] Job Type dropdown has **no pre-selected default** (fix #3)
- [ ] `SELECT id,status,job_type,error_message FROM destinations WHERE status='error'` shows the old Barcelona/Lisbon rows auto-reset by stale recovery (fix #2)
- [ ] From an outside network: `nmap NEW_DROPLET_IP` shows no direct 80/443 (only Cloudflare reaches origin), 22 filtered except from `MYIP`
- [ ] `sudo fail2ban-client status sshd` active; `sudo ss -tlnp` shows MySQL/Redis/8000 on 127.0.0.1 only
- [ ] `php artisan about` shows `Environment: production`, `Debug Mode: OFF`

Cutover & decommission:
- [ ] Update the client with the new `https://DOMAIN`
- [ ] Old droplet: it already has the evidence snapshot. Power it **off** (don't destroy yet), keep the snapshot **30 days**, then destroy droplet + snapshot.
- [ ] Confirm every row in the Phase 0 rotation table is ☑.

---

## Phase 9 — Ongoing

- DO → Monitoring: alert on CPU > 80% / bandwidth spikes (catches a miner early).
- Weekly: `composer audit` and `pip-audit` (cron a report to yourself).
- Backups: nightly `mysqldump diventoscrapper` pushed to **DO Spaces / offsite**
  — NOT just droplet snapshots (a post-compromise snapshot carries the malware).
- If anything in `storage/` or `public/` gets an unexpected `.php`, the Apache
  block makes it inert, but treat it as an incident and check Access logs.
```
