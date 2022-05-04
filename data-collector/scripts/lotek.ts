import needle from 'needle';
import 'dotenv/config';
import { getIsDuplicateAlert, getLastAlertTimestamp, getLastKnownLatLong, isPoolEmpty, pgPool, pgPoolEndAsync, queryAsync } from './utils/db';
const dayjs = require('dayjs');
import { eVendorType, retrieveCredentials, ToLowerCaseObjectKeys  } from './utils/credentials';
import { ICollar, ILotekAlert, LOTEK_TEST_ALERTS } from './types/lotek';
import { formatNowUtc, nowUtc } from './utils/time';
import { performance } from 'perf_hooks';
import { data } from 'cypress/types/jquery';

//Enabled in dev for local testing. Uses LOTEK_TEST_ALERTS in types/lotek.ts
const TESTMODE: boolean = process.env.POSTGRES_SERVER_HOST === 'localhost';

const ALERT_TABLE = 'telemetry_sensor_alert';

// Store the access token globally
let tokenConfig = {};
let lotekUrl: string;

//Extending the console.log to start with UTC time.

var log = console.log;
console.log = function(){
  var args = Array.from(arguments);
  args.unshift(formatNowUtc() + ": ");
  log.apply(console, args);
}



/* ## insertCollarData
Insert collar data into the database.
This is run under and asyncJS series loop. If there is an error, pass it to the callback as the first parameter.
@param records {object} Array object of collar records
@param callback {function} The asyncJS callback function. Provide an error message if something fails, otherwise null.
 */
const insertCollarData = async function (records) {
  const sql = `select vendor_insert_raw_lotek('[${records
    .map((v) => JSON.stringify(ToLowerCaseObjectKeys(v)))
    .join()}]')`;
  console.log(`Entering ${records.length} records for collar ${records[0].DeviceID}`);

  await pgPool.query(sql).catch(err => console.log(err))
};

/* ## iterateCollars
  This function gets called for each collar in series.
  Creates a query to the API containing the ID of the collar.
  The results get passed to the *insertCollarData* function along with
  the asyncJS callback function to be called on completion of collar operations.
  @param collar {object} The collar object containing the unique ID.
  @param callback {function} The asyncJS callback function. Provide an error message if something fails, otherwise null.
 */
const iterateCollars = async function (collar: ICollar) {
  const weekAgo = dayjs().subtract(7, 'd').format('YYYY-MM-DDTHH:mm:ss');
  const url = `${lotekUrl}/gps?deviceId=${collar.nDeviceID}&dtStart=${weekAgo}`;

  // Send request to the API
  const { body, error } = await needle('get', url, tokenConfig);
  if (error) {
    const msg = `Could not get collar data for ${collar.nDeviceID}: ${error}`;
    console.log(msg);
    // throw new Error(`DeviceID ${collar.nDeviceID} ${error}`)
    return;
  }

  if (!body.flat) {
    const msg = `Did not receive a valid array for ${
      collar.nDeviceID
    } body: ${JSON.stringify(body)}`;
    console.log(msg);
    return;
  }

  const records = body.flat().filter((e) => {
    return e && e.RecDateTime && e.DeviceID;
  });

  if (records.length < 1) {
    const msg = `No records for ${collar.nDeviceID}`;
    console.log(msg);
    return;
  }
  if(TESTMODE) throw new Error('Test error throw');
  await insertCollarData(records)
    .catch(() => console.log(`Error inserting ${collar.nDeviceID} data into DB`))
};

/* ## getAllCollars
  Request all collars through the API
  @param err {string} Error message if there is one. Null if not
  @param _ {object} The network request object. Blanked
  @param data {object} The array object of all collars, containing IDs
 */
const getAllCollars = async function () {

  const url = `${lotekUrl}/devices`; // the API url
  const { body, error } = await needle('get', url, tokenConfig);
  if (error) {
    return console.log('Could not get collar list: ', error);
  }
  /*
    Async workflow of cycling through all the collars.
    Run the IterateCollars function on each collar.
    When done. Shut down the database connection.
  */
  if (body) {
    await Promise.all<ICollar>(
      body.map(async (collar: ICollar, i: number, promiseArr: ICollar[]) => {
        await iterateCollars(collar)
        //Individual insert error handling
          .then(() => {
            // console.log('Success');
          })
          .catch(async (err) => {
            console.log(`DeviceID ${collar.nDeviceID} ${err}`);
            // await iterateCollars(collar)
            //   .catch(err => console.log(`DeviceID ${collar.nDeviceID} failed for second time...`))
          })
    }))
    .catch(err => console.log(err));
  }
  

};


//Tests alert is of proper type
const confirmAlertType = (alert: ILotekAlert) => alert.strAlertType === ('Mortality' || 'Malfunction');

/**
 * fetches Lotek alerts, filters out any alerts that are alerts older than either: 
 *  a) the last Lotek alert added to the telemetry table. 
 *  b) if no lotek alerts were found in the alert table, returns alerts fetched within the past week (7 days)
 */
const getAlerts = async () => {
  const url = `${lotekUrl}/alerts`;
  let { body, error } = await needle<ILotekAlert[]>('get', url, tokenConfig);
  if (error) {
    console.log(`error retrieving results from Lotek alert API: ${error}`);
    return;
  }
  const lastAlert = await getLastAlertTimestamp(ALERT_TABLE, eVendorType.lotek) ?? dayjs().subtract(7, 'd').format('YYYY-MM-DDTHH:mm:ss');

  if(TESTMODE) body = LOTEK_TEST_ALERTS;

  let filtered: ILotekAlert[] = body.filter((alert) => dayjs(alert.dtTimestamp).isAfter(dayjs(lastAlert)) && confirmAlertType(alert));
  
    console.log( `alerts fetched: ${body.length}, new alerts count: ${filtered.length}`);
    if (filtered.length == 0) {
      return;
    }
    insertAlerts(filtered);
};

/**
 * insert new alerts directly to the telemetry_sensor_alert table
 */
const insertAlerts = async (alerts: ILotekAlert[]) => {
  /**
   * All alerts have this field, and many have it set to this time.
   * Assumption is the alert has not been cancelled if it matches this timestamp.
   */
  const timestampNotCanceled = '0001-01-01T00:00:00';

  const sqlPreamble = `
    insert into bctw.telemetry_sensor_alert (
      "device_id",
      "device_make",
      "alert_type",
      "latitude",
      "longitude",
      "valid_from"
    ) values
  `;
  const newAlerts: string[] = [];
  for (const alert of alerts) {
    let {nDeviceID, dtTimestamp, dtTimestampCancel, strAlertType, latitude, longitude} = alert;
    
    // if there is already an alert for this device, skip it
    const isDuplicateAlert = await getIsDuplicateAlert(ALERT_TABLE, nDeviceID, eVendorType.lotek, strAlertType)
      .then(data => data)
      .catch(err => 
        console.log(`DeviceId: ${nDeviceID} + Alert: ${strAlertType} - Error in 'getIsDuplicateAlert()': ${err}`))
    if (isDuplicateAlert) {
      console.log(`${strAlertType} alert with DeviceId ${nDeviceID} already exists in alert table. Skipping...`);
      continue;
    }
    
    //Checks if Lotek sent (0,0) coords.
    if(!latitude || !longitude){
      
      //Checks if the database has valid coords for this deviceID.
      const coords = await getLastKnownLatLong(nDeviceID, eVendorType.lotek, dtTimestamp)
      .then(data => data)
      .catch(err => {console.log(`DeviceId: ${nDeviceID} + Alert: ${strAlertType} - GetLastKnowLatLong failed.`, err)})
      
      //Set lat long to valid coords.
      if(coords){
        latitude = coords.latitude;
        longitude = coords.longitude;
      }else{
        latitude = null;
        longitude = null;
      }
      console.log(`DeviceId: ${nDeviceID} has coords(0,0) -> setting to last known location or nulls -> (${latitude},${longitude})`);
      }
      
      //Pushes the alert to the newAlerts array.
      if (dtTimestampCancel === timestampNotCanceled) {
          newAlerts.push(
            `(${nDeviceID},
            '${eVendorType.lotek}',
            '${strAlertType.toLowerCase()}',
            ${latitude},
            ${longitude},
            '${dtTimestamp}')`
            );
          }
  }
      

  if (!newAlerts.length) {
    console.log('no new Lotek alerts detected');
    return;
  }

  const sql = sqlPreamble + newAlerts.join(',');
  console.log(`Inserting ` + newAlerts.length + " alert records");
  console.log(`valid alerts found ${JSON.stringify(alerts)}`)
  await queryAsync(sql)
    .catch(err => `Error inserting alert into DB ${err}`)
};

// sets token retrieved from login globally
const setToken = (data) => {
  tokenConfig = {
    headers: { Authorization: `bearer ${data.access_token}` },
  };
};

/**
 * Get the authentication token from the API
 * Feed the token into the collar aquisition and iteration function
*/
const main = async function () {
  if(TESTMODE) console.log(`TEST MODE ENABLED: ${LOTEK_TEST_ALERTS.length} test alerts.`);
  console.log('Lotek CronJob: V1.9.6');
  pgPool.on('error', (err) => {
    console.log('Pool Error: ', err);
  })
  const credential_name_id = process.env.LOTEK_API_CREDENTIAL_NAME;
  if (!credential_name_id) {
    console.log(`credential identifier: 'LOTEK_API_CREDENTIAL_NAME' not supplied`)
    return;
  }
  // retrieve the lotek credentials from the encrypted db table
  const { username, password, url } = await retrieveCredentials(credential_name_id);
  if (!url) {
    console.log(`unable to retrieve Lotek vendor credentials using identifier ${credential_name_id}`)
    return;
  }
  // set the global API url variable
  lotekUrl = url;

  const data = `username=${username}&password=${password}&grant_type=password`;
  const loginURL = `${url}/user/login`;
  const config = { content_type: 'application/x-www-form-urlencoded' };

  const { body, error } = await needle('post', loginURL, data, config);
  if (error) {
    console.log(`unable to retrieve lotek API login token ${error}`);
  }
  setToken(body);

  await getAlerts()
    .then(() => console.log(`PHASE[1] (getAlerts) COMPLETE`))
    .catch(err => console.log(`Error in PHASE-1 (getAlerts)`, err))

  await getAllCollars()
  .then(()=> console.log(`PHASE[2] (getCollars) COMPLETE...`))
  .catch(err => console.log(`Error in PHASE[2] (getCollars)`, err))

  

  // while (true){
  //   let noConnections = !pgPool.totalCount && !pgPool.idleCount && !pgPool.waitingCount;
  //   console.log(`PG Pool Details: totalCount: ${pgPool.totalCount} | idleCount: ${pgPool.idleCount} | waitingCount: ${pgPool.waitingCount}`)
  //   if(!pgPool.totalCount && !pgPool.idleCount && !pgPool.waitingCount){
  //     console.log('Closing database connection');
  //     console.log(`Process took ${(performance.now() - START_TIMER) / 1000} seconds 🦌`);
  //     pgPool.end();
  //     break;
  //   }
  // } 
};



/*
  Entry point - Start script
 */
(async () => {
  const START_TIMER = performance.now();
  try {
    //Run main script
    const run = await main();
  } catch (e) {
      // Deal with the fact the script failed
      console.log('Error occured within the script', e);

  } finally {
      //Close database connection
      let RETRIES = 3;
      while(true){
        if(!RETRIES){
          console.log(`DB tried 3 safe times to close and was unsuccessful...`);
          break;
        }
        const emptyPool = await isPoolEmpty().then(res => res);
        if(emptyPool){
          console.log('PHASE[3] drain pool COMPLETE')
          break;
        }else{
          RETRIES--;
        }
      }
    await pgPool.end()
      .then(()=>console.log(`PHASE[4] Closing database connection... COMPLETE`))
      .catch(err => console.log(`Problem occured when ending pool...`, err));
    console.log(`Process took ${(performance.now() - START_TIMER) / 1000} seconds 🦌`);
  }
})();
//getToken();
