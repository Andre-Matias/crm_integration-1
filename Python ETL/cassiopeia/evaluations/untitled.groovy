import java.text.SimpleDateFormat
import java.util.Date

node {
    try {
        stage('checkout code') {

            // Get fresh code from our GitHub repository for job logic
                dir('eu-yamato') {
                    git url: 'git@github.com:naspersclassifieds-regional/eu-yamato.git'
                }

        }

            stage('run ad traffic for verticals') {

            int value = 1
            String month = '12';
            String day = '01';
            String year = '2017';
            
            sh "python3.6 eu-yamato/tools/spedytor/code/spedytor.py --date_from ${year}-${month}-${day} --date_to ${year}-${month}-${day} --run_mode UL --source yamato --target yamato --target_table miguel_chin.ad_traffic_verticals_pv --s3prefix live-temp-eu --s3prefixsub vas_hydra_verticals --filename vas_ad_traffic_pv --credentials brak --sql eu-yamato/tools/spedytor/scripts/vas_ad_traffic_verticals_pv.sql --sql_columns brak --force_date_hierarchy N --slack_login \"\" --hive_columns_types brak --extra RunAsAdmin"
        
        
        }
        
        
        
        stage('Pagerduty') {
            mail (
                from: 'noreply@onap.io',
                to: 'yamato-developers@olx.com',
            /*subject: "PASS: ${env.JOB_NAME} #${env.BUILD_NUMBER}",*/
                subject: "Successful job: ${env.JOB_NAME} #${env.BUILD_NUMBER}",
                body: "ETL has finished successfully! ",
                mimeType:'text/html');
            deleteDir()
        }
        
        
    }
    catch (err){
        stage 'Pagerduty'
        mail (
            from: 'noreply@onap.io',
            to: 'yamato-developers@olx.com',
            subject: "Failed job: ${env.JOB_NAME} #${env.BUILD_NUMBER}",
            body: "It appears that ${env.BUILD_URL} is failing, somebody should do something about that",
            mimeType:'text/html'
            );
        currentBuild.result = 'FAILURE'
        deleteDir()
    }
}


