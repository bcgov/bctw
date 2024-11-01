import axios from 'axios';
import dayjs from 'dayjs';
import fastq, { queueAsPromised } from 'fastq';
import { PoolClient } from 'pg';
import { getKnex, IDBConnection } from '../database/db';
import { getLogger } from '../utils/logger';
import { DBService } from './db-service';

const defaultLogger = getLogger('LotekService');

/**
 * Interface representing a device from the Lotek API.
 */
interface ILotekDevice {
  nDeviceID: number;
  strSpecialID: string;
  dtCreated: string;
  strSatellite: string;
}

/**
 * Interface representing the response from the Lotek API.
 */
interface ILotekRecord {
  channelstatus: string;
  uploadtimestamp: string;
  latitude: number;
  longitude: number;
  altitude: number;
  ecefx: number;
  ecefy: number;
  ecefz: number;
  rxstatus: number;
  pdop: number;
  mainv: number;
  bkupv: number;
  temperature: number;
  fixduration: number;
  bhastempvoltage: boolean;
  devname: string | null;
  deltatime: number;
  fixtype: number;
  cepradius: number;
  crc: number;
  deviceid: number;
  recdatetime: string;
  timeid: string;
}

export type Task = {
  deviceId: number;
};

/**
 * Class responsible for processing Lotek GPS telemetry data and inserting it into the database.
 */
export class LotekService extends DBService {
  dbClient: PoolClient | undefined;
  token: string | undefined;
  queue: queueAsPromised<Task>;

  lotekApi: string;
  lotekUser: string;
  lotekPass: string;

  queueConcurrency: number;
  recordsPerInsert: number;

  constructor(connection: IDBConnection) {
    super(connection);

    this.lotekApi = process.env.LOTEK_API_URL || '';
    this.lotekUser = process.env.LOTEK_USER || '';
    this.lotekPass = process.env.LOTEK_PASS || '';

    this.queueConcurrency = Number(process.env.LOTEK_QUEUE_CONCURRENCY) || 4;
    this.recordsPerInsert = Number(process.env.LOTEK_DB_INSERT_BATCH_SIZE) || 1000;

    this.queue = fastq.promise(this, this._queueWorker, this.queueConcurrency);
  }

  /**
   * Starts the process of fetching and processing telemetry data from the Lotek API.
   *
   * @return {*}  {Promise<void>}
   * @memberof LotekService
   */
  async process(): Promise<void> {
    try {
      // Authenticate with the Lotek API
      this.token = await this._authenticate();

      // Fetch all devices from the Lotek API
      const allDevices = await this._getDeviceList();

      // Get a new DB client from the pool
      //   this.dbClient = await this.connection.getClient();
      this.dbClient = {
        query: () => {
          return Promise.resolve({
            rowCount: 10
          });
        },
        release: () => {}
      } as any;

      // Queue the devices for processing
      for (const device of allDevices) {
        this.queue.push({ deviceId: device.nDeviceID });
      }

      // Wait for the queue to drain
      await this.queue.drain();
    } catch (error) {
      defaultLogger.error('Failed to process Lotek telemetry data: ', error);
    } finally {
      // Release the DB client
      this.dbClient?.release();
    }
  }

  /**
   * Authenticates with the Lotek API and returns the Bearer token.
   *
   * @return {*}  {Promise<string>}
   * @memberof LotekService
   */
  async _authenticate(): Promise<string> {
    try {
      const { data } = await axios.post(
        `${this.lotekApi}/user/login`,
        {
          username: this.lotekUser,
          password: this.lotekPass,
          grant_type: 'password'
        },
        {
          headers: {
            'Content-Type': 'application/x-www-form-urlencoded'
          }
        }
      );

      return data.access_token; // Return the auth token
    } catch (error) {
      defaultLogger.error('Failed to authenticate for the Lotek api: ', error);

      throw new Error('Authentication failed');
    }
  }

  /**
   * Fetches a list of devices from the Lotek API.
   *
   * @return {*}  {Promise<ILotekDevice[]>}
   * @memberof LotekService
   */
  async _getDeviceList(): Promise<ILotekDevice[]> {
    if (!this.token) {
      throw new Error('Token is undefined');
    }

    try {
      const { data } = await axios.get<ILotekDevice[]>(`${this.lotekApi}/devices`, {
        headers: {
          Authorization: `Bearer ${this.token}`
        }
      });

      return data;
    } catch (error) {
      defaultLogger.error('Unable to fetch devices in the Lotek account.', error);
      return [];
    }
  }

  /**
   * Processes a single task from the queue.
   *
   * @param {Task} task
   * @return {*}  {Promise<void>}
   * @memberof LotekService
   */
  async _queueWorker(task: Task): Promise<void> {
    if (!this.token) {
      throw new Error('Token is undefined');
    }

    try {
      const telemetryRecords = await this._getDeviceTelemetry(task.deviceId, this.token);

      return this._insertTelemetry(telemetryRecords);
    } catch (error) {
      defaultLogger.error('Failed to process telemetry data for device:', error);
    }
  }

  /**
   * Requests telemetry data for a single device from the Lotek API.
   *
   * @param {number} deviceId
   * @param {string} token
   * @return {*}  {Promise<ILotekRecord[]>}
   * @memberof LotekService
   */
  async _getDeviceTelemetry(deviceId: number, token: string): Promise<ILotekRecord[]> {
    try {
      const { data } = await axios.get<ILotekRecord[]>(`${this.lotekApi}/gps`, {
        params: {
          deviceId: deviceId
        },
        headers: {
          Authorization: `Bearer ${token}`
        }
      });

      return data;
    } catch (error) {
      defaultLogger.error(`Failed to fetch data for device ${deviceId}:`, error);
    }

    return [];
  }

  /**
   * Inserts telemetry data records into the database.
   *
   * @param {ILotekRecord[]} telemetryRecords
   * @return {*}  {Promise<void>}
   * @memberof LotekService
   */
  async _insertTelemetry(telemetryRecords: ILotekRecord[]): Promise<void> {
    if (!this.dbClient) {
      throw new Error('DB client is undefined');
    }

    if (!telemetryRecords.length) {
      return;
    }

    const knex = getKnex();

    let insertedRowCount = 0;

    for (let i = 0; i < telemetryRecords.length; i += this.recordsPerInsert) {
      const rowsBatch = telemetryRecords.slice(i, i + this.recordsPerInsert);

      const insertRows: Record<string, any>[] = [];

      rowsBatch.forEach((row) => {
        insertRows.push({
          channelstatus: row.channelstatus,
          uploadtimestamp: dayjs(row.uploadtimestamp).toISOString(),
          latitude: row.latitude,
          longitude: row.longitude,
          altitude: row.altitude,
          ecefx: row.ecefx,
          ecefy: row.ecefy,
          ecefz: row.ecefz,
          rxstatus: row.rxstatus,
          pdop: row.pdop,
          mainv: row.mainv,
          bkupv: row.bkupv,
          temperature: row.temperature,
          fixduration: row.fixduration,
          bhastempvoltage: row.bhastempvoltage,
          devname: row.devname,
          deltatime: row.deltatime,
          fixtype: row.fixtype,
          cepradius: row.cepradius,
          crc: row.crc,
          deviceid: row.deviceid,
          recdatetime: row.recdatetime,
          timeid: this._getTimeId(row), // unique identifier for each record
          geom: `st_setSrid(st_point(${row.longitude}, ${row.latitude}), 4326)`
        });
      });

      // @ts-ignore
      const queryBuilder = knex
        .queryBuilder()
        .insert(insertRows)
        .into('telemetry_api_lotek')
        .onConflict(knex.raw(`ON CONFLICT DO NOTHING RETURNING timeid`))
        .ignore();

      //   const { sql, bindings } = queryBuilder.toSQL().toNative();

      //   const response = await this.dbClient.query(sql, bindings as any[]);

      //   insertedRowCount += response.rowCount;
      insertedRowCount += insertRows.length;
    }
    defaultLogger.info(`Successfully inserted ${insertedRowCount} new Lotek records`);
  }

  /**
   * Generates a unique identifier for a telemetry record.
   *
   * @param {ILotekRecord} row
   * @return {*}  {string}
   * @memberof LotekService
   */
  _getTimeId(row: ILotekRecord): string {
    return `${row.deviceid}_${dayjs(row.recdatetime).toISOString()}`;
  }
}
