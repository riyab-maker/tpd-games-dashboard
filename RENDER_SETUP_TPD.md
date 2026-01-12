# Render Deployment Setup for TPD Games Dashboard

## Prerequisites

- ✅ GitHub repository created and pushed (see `GITHUB_SETUP_TPD.md`)
- ✅ All data files processed and committed to repository
- ✅ Render account created (sign up at https://render.com)

## Step-by-Step Deployment

### Step 1: Create Render Account (if needed)

1. Go to https://render.com
2. Click "Get Started for Free"
3. Sign up with GitHub (recommended) or email
4. Verify your email if required

### Step 2: Connect GitHub Account

1. In Render dashboard, go to "Account Settings"
2. Click "Connect GitHub" (if not already connected)
3. Authorize Render to access your repositories
4. Select the repositories you want to deploy (or "All repositories")

### Step 3: Create New Web Service

1. In Render dashboard, click **"New +"** button
2. Select **"Web Service"**
3. Connect your GitHub account if prompted
4. Select the repository: `tpd-games-dashboard` (or your repository name)

### Step 4: Configure Service Settings

Fill in the following settings:

#### Basic Settings
- **Name**: `tpd-games-dashboard` (or your preferred name)
- **Region**: Choose closest to your users (e.g., `Oregon (US West)`)
- **Branch**: `main` (or your default branch)
- **Root Directory**: Leave empty (or specify if files are in subdirectory)
- **Environment**: `Python 3`
- **Build Command**: `pip install -r requirements.txt`
- **Start Command**: `streamlit run src/streamlit_dashboard_tpd.py --server.port=$PORT --server.address=0.0.0.0`

#### Advanced Settings (Optional)
- **Plan**: `Free` (or upgrade to paid plan for better performance)
- **Auto-Deploy**: `Yes` (automatically deploy on git push)

### Step 5: Set Environment Variables

1. Scroll down to **"Environment Variables"** section
2. Click **"Add Environment Variable"** for each:

```
REDSHIFT_HOST=your-redshift-host
REDSHIFT_DATABASE=your-database-name
REDSHIFT_PORT=5439
REDSHIFT_USER=your-username
REDSHIFT_PASSWORD=your-password
PYTHON_VERSION=3.11.0
```

**Important**: 
- Replace placeholder values with your actual credentials
- Keep these values secure - never commit them to GitHub
- Use Render's environment variables for sensitive data

### Step 6: Deploy

1. Review all settings
2. Click **"Create Web Service"**
3. Render will:
   - Clone your repository
   - Install dependencies
   - Build the application
   - Start the service

### Step 7: Monitor Deployment

1. Watch the build logs in real-time
2. Check for any errors:
   - **Build errors**: Usually dependency or configuration issues
   - **Runtime errors**: Check application logs
3. Wait for deployment to complete (usually 2-5 minutes)

### Step 8: Access Your Dashboard

Once deployed:
- Your dashboard will be available at: `https://tpd-games-dashboard.onrender.com`
- The URL format is: `https://[service-name].onrender.com`
- Free tier services may spin down after inactivity (takes ~30 seconds to wake up)

## Configuration Files

### render_tpd.yaml (Alternative Method)

If you prefer using `render_tpd.yaml`:

1. Ensure `render_tpd.yaml` is in your repository root
2. In Render, when creating service:
   - Select **"Apply Render YAML"** option
   - Render will read configuration from `render_tpd.yaml`
   - You can still override settings in the UI if needed

The `render_tpd.yaml` file contains:
```yaml
services:
  - type: web
    name: tpd-games-dashboard
    env: python
    plan: free
    buildCommand: pip install -r requirements.txt
    startCommand: streamlit run src/streamlit_dashboard_tpd.py --server.port=$PORT --server.address=0.0.0.0
    envVars:
      - key: PYTHON_VERSION
        value: 3.11.0
```

**Note**: Environment variables with sensitive data (like passwords) should still be set in the Render UI, not in the YAML file.

## Updating the Dashboard

### Manual Update

1. Make changes locally
2. Run preprocessing if data changed:
   ```bash
   python preprocess_data_tpd.py --all
   ```
3. Commit and push:
   ```bash
   git add .
   git commit -m "Update dashboard"
   git push
   ```
4. Render will automatically detect changes and redeploy

### Automatic Updates

Set up GitHub Actions (see `GITHUB_SETUP_TPD.md`) to automatically:
- Run preprocessing daily
- Update data files
- Trigger Render redeployment

## Troubleshooting

### Build Fails

**Error**: "Module not found" or "Import error"
- **Solution**: Check `requirements.txt` includes all dependencies
- Verify all imports in `streamlit_dashboard_tpd.py` are available

**Error**: "File not found" or "CSV not found"
- **Solution**: Ensure all data files are committed to repository
- Check file paths in the code match repository structure

### Runtime Errors

**Error**: "No data available"
- **Solution**: 
  - Verify data files exist in `data/` directory
  - Check that preprocessing was run before deployment
  - Verify `tpd_conversion_funnel.csv` exists

**Error**: "Database connection failed"
- **Solution**: 
  - Verify environment variables are set correctly
  - Check Redshift credentials
  - Ensure Redshift allows connections from Render's IPs

### Service Won't Start

**Error**: "Port already in use" or "Address already in use"
- **Solution**: This shouldn't happen with `$PORT` variable, but check start command

**Error**: "Streamlit not found"
- **Solution**: Verify `streamlit` is in `requirements.txt`

### Performance Issues

**Slow loading**:
- Free tier has limited resources
- Consider upgrading to paid plan
- Optimize data file sizes
- Use data compression

**Service spins down**:
- Free tier services spin down after 15 minutes of inactivity
- First request after spin-down takes ~30 seconds
- Upgrade to paid plan to avoid spin-downs

## Monitoring

### View Logs

1. Go to your service in Render dashboard
2. Click **"Logs"** tab
3. View:
   - Build logs (deployment)
   - Runtime logs (application)

### Metrics

Render provides metrics for:
- CPU usage
- Memory usage
- Request count
- Response times

Access via the **"Metrics"** tab in your service.

## Custom Domain (Optional)

1. Go to service settings
2. Click **"Custom Domains"**
3. Add your domain
4. Follow DNS configuration instructions
5. Render will provide SSL certificate automatically

## Cost Considerations

### Free Tier
- ✅ 750 hours/month (enough for always-on service)
- ✅ 512MB RAM
- ✅ Automatic SSL
- ⚠️ Services spin down after inactivity
- ⚠️ Limited resources

### Paid Plans
- Better performance
- No spin-downs
- More resources
- Better for production use

## Security Best Practices

1. **Never commit secrets** to GitHub
2. **Use environment variables** for all sensitive data
3. **Enable 2FA** on Render account
4. **Review access logs** regularly
5. **Keep dependencies updated**

## Support

- Render Documentation: https://render.com/docs
- Render Support: support@render.com
- Community: https://community.render.com

## Next Steps

After successful deployment:
1. Test all dashboard features
2. Set up monitoring
3. Configure custom domain (optional)
4. Set up automated data updates
5. Share dashboard URL with stakeholders
