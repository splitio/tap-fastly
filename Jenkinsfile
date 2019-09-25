#!/usr/bin/env groovy

pipeline {
    agent any

    triggers {
      cron('H * * * *') // run every hour
    }

    options {
      buildDiscarder(logRotator(daysToKeepStr: '2'))
    }

    environment {
      SF_ROLE="SYSADMIN"
      SF_DATABASE="SPLIT"
      SF_WAREHOUSE="COMPUTE_WH"
      SF_CRED=credentials("SNOWFLAKE")
      SF_ACCOUNT="bv23770.us-east-1"

      // State File
      FASTLY_STATE="./states/fastly.json"

      // Python Enviroments
      VENV_FASTLY="venv/tap-fastly"
      VENV_SF="venv/target-snowflake"

    }

    stages {

        stage('Create States directory') {
          steps {
            sh "mkdir -p ./states"
          }
        } // Stage States Directory

        stage('Create Venvs') {
          parallel {
            stage('Venv Fastly') {
              environment {
                SOURCE_INSTALL='.[dev]'
                FLAG="-e"
              }
              steps {
                sh './createVenv.sh "${VENV_FASTLY}" "${SOURCE_INSTALL}" "${FLAG}"'
              }
            }// stage Venv Fastly
            stage('Venv Snowflake') {
              environment {
                SOURCE_INSTALL='git+https://gitlab.com/meltano/target-snowflake.git@master#egg=target-snowflake'
                FLAG="-e"
              }
              steps {
                sh './createVenv.sh "${VENV_SF}" "${SOURCE_INSTALL}" "${FLAG}"'
              }
            } // Stage Venv Snowflake
            stage('State Fastly'){
              steps{
                setState("${FASTLY_STATE}")
              }
            }// stage State Fastly
          } // Parallel
        } // Stage Create Venv

        stage('Run Tap-fastly'){
          environment{
            FASTLY_START_DATE="2017-07-01"// for billing information
            FASTLY_TOKEN=credentials('FASTLY_TOKEN')
            SF_SCHEMA="FASTLY"
            SF_CONFIG_FILE="config-snowflake-fastly.json"
            TAP_OUTPUT="tap-fastly-output.json"
            STDERRFILE="stderr_fastly.out"
          }
          steps{
            script{
                sh(returnStdout: false, script: 'set -euo pipefail')
                sh(returnStdout: false, script: 'envsubst < config-fastly.json.tpl > config-fastly.json')
                sh(returnStdout: false, script: 'envsubst < config-snowflake.json.tpl > "${SF_CONFIG_FILE}"')
                status=sh(returnStatus: true, script: '${VENV_FASTLY}/bin/tap-fastly -c config-fastly.json --catalog fastly-properties.json -s "${FASTLY_STATE}" > "${TAP_OUTPUT}" 2>"${STDERRFILE}"')
                catchError(status, "Tap-fastly", "Failed to collect data.", "${STDERRFILE}")
                status=sh(returnStdout: false, script:'echo -e "\n" >>  ${FASTLY_STATE}')
                status=sh(returnStatus: true, script: 'cat ${TAP_OUTPUT} | ${VENV_SF}/bin/target-snowflake -c "${SF_CONFIG_FILE}" >> ${FASTLY_STATE} 2>"${STDERRFILE}"')
                catchError(status, "Tap-fastly", "Failed to send data.", "${STDERRFILE}")
            }
          }
        }// stage Run Tap-fastly

    } // Stages

    post{

      success{
        slackSend(channel: "#analytics-alerts", message: "Tap-fastly Worked.", color: "#008000")
      }
      always{
        cleanWs (
          deleteDirs: false,
          patterns: [
            [pattern: 'config*.json', type: 'INCLUDE'],
            [pattern: '*output*.json', type: 'INCLUDE'],
            [pattern: 'stderr*.out', type: 'INCLUDE']
          ]
        )
      }//always
    }// post
} // Pipeline

def setState(state){
  def exists = fileExists state
  if (exists) {
    def file = readFile state
    def last = file.split("\n")[file.split("\n").length-1]
    writeFile file: state, text : last
    def count = sh(returnStdout:true, script:'cat '+ state + ' | tr \' \' \'\n\' | grep bookmark | wc -l').trim()
    echo count
    sh(returnStdout:true, script:'cat ' + state)
  }
  else {
    writeFile file: state, text: '{}'
  }
}

def catchError(status, tap, message, stderrfile){
  if (status != 0) {
    def output = readFile(stderrfile)
    print(output)
    slackSend(channel: "#analytics-alerts", message: "*$tap:* $message \n *Reason:* $output", color: "#ff0000")
    currentBuild.result = 'FAILED'
    error "$message"
  }
}
