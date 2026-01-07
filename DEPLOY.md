# Deployment Guide: Render

Your project is fully configured for deployment on Render.

## 1. Create Web Service
1.  Go to [Render Dashboard](https://dashboard.render.com/).
2.  Click **New +** -> **Web Service**.
3.  Select **Build and deploy from a Git repository**.
4.  Connect your repo: `Hrishikesh-Prasad-R/QualityScoringInPaymentGateway`.

## 2. Configuration
Render will detect the settings, but verify:
-   **Runtime**: Python 3
-   **Build Command**: `pip install -r requirements.txt`
-   **Start Command**: `gunicorn --worker-class eventlet -w 1 --bind 0.0.0.0:$PORT app:app` (or it will auto-detect from `Procfile`)

## 3. Environment Variables (Critical)
You MUST set the Gemini API Key for the AI features to work.
1.  Scroll down to **Environment Variables**.
2.  Add Key: `GEMINI_API_KEY`
3.  Value: `[Your Gemini API Key]`

## 4. Deploy
Click **Create Web Service**. 
The build will take 1-2 minutes. Once active, your dashboard will be live!
