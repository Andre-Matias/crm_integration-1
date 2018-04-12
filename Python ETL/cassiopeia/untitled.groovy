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
        
        stage('run ad impressions for verticals web') {

            for (i = 3; i < 11; i++) {
                
                String month = '12';
                String day = i.toString().padLeft(2,'0');
                String year = '2017';

                sh "python3.6 eu-yamato/tools/spedytor/code/spedytor.py --date_from ${year}-${month}-${day} --date_to ${year}-${month}-${day} --run_mode R --source yamato --target yamato --target_table brak --s3prefix brak --s3prefixsub brak --filename brak --credentials brak --sql eu-yamato/tools/spedytor/scripts/vas_scripts/vas_ad_traffic_verticals_imp_web.sql --sql_columns brak --force_date_hierarchy N --slack_login \"\" --hive_columns_types brak --extra RunAsAdmin"
            }
            

        }
        stage('run ad impressions for verticals html5') {

            for (i = 3; i < 11; i++) {
                
                String month = '12';
                String day = i.toString().padLeft(2,'0');
                String year = '2017';
                
                sh "python3.6 eu-yamato/tools/spedytor/code/spedytor.py --date_from ${year}-${month}-${day} --date_to ${year}-${month}-${day} --run_mode R --source yamato --target yamato --target_table brak --s3prefix brak --s3prefixsub brak --filename brak --credentials brak --sql eu-yamato/tools/spedytor/scripts/vas_scripts/vas_ad_traffic_verticals_imp_web_html5.sql --sql_columns brak --force_date_hierarchy N --slack_login \"\" --hive_columns_types brak --extra RunAsAdmin" 
            }
            

        }

        stage('run ad impressions for verticals android') {

            for (i = 3; i < 11; i++) {
                
                String month = '12';
                String day = i.toString().padLeft(2,'0');
                String year = '2017';
            
                sh "python3.6 eu-yamato/tools/spedytor/code/spedytor.py --date_from ${year}-${month}-${day} --date_to ${year}-${month}-${day} --run_mode R --source yamato --target yamato --target_table brak --s3prefix brak --s3prefixsub brak --filename brak --credentials brak --sql eu-yamato/tools/spedytor/scripts/vas_scripts/vas_ad_traffic_verticals_imp_and.sql --sql_columns brak --force_date_hierarchy N --slack_login \"\" --hive_columns_types brak --extra RunAsAdmin"
            }

        }

        stage('run ad impressions for verticals ios') {

            for (i = 3; i < 11; i++) {
                
                String month = '12';
                String day = i.toString().padLeft(2,'0');
                String year = '2017';
            
                sh "python3.6 eu-yamato/tools/spedytor/code/spedytor.py --date_from ${year}-${month}-${day} --date_to ${year}-${month}-${day} --run_mode R --source yamato --target yamato --target_table brak --s3prefix brak --s3prefixsub brak --filename brak --credentials brak --sql eu-yamato/tools/spedytor/scripts/vas_scripts/vas_ad_traffic_verticals_imp_ios.sql --sql_columns brak --force_date_hierarchy N --slack_login \"\" --hive_columns_types brak --extra RunAsAdmin"
            }
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


