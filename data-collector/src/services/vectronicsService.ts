import axios from 'axios';
import dayjs from 'dayjs';
import fastq, { queueAsPromised } from 'fastq';
import { PoolClient } from 'pg';
import { getKnex, IDBConnection } from '../database/db';
import { getLogger } from '../utils/logger';
import { DBService } from './db-service';

const defaultLogger = getLogger('VectronicService');

/**
 * Interface representing a device credential from the database, where collarkey authorizes the request.
 */
interface IDeviceCredential {
  idcollar: number;
  collarkey: string;
}

/**
 * Interface representing the response from the Vectronic API.
 */
interface IVectronicRecord {
  idPosition: number;
  idCollar: number;
  acquisitionTime: string;
  scts: string;
  originCode: string;
  ecefX: number;
  ecefY: number;
  ecefZ: number;
  latitude: number;
  longitude: number;
  height: number;
  dop: number;
  idFixType: number;
  positionError: number | null;
  satCount: number;
  ch01SatId: number | null;
  ch01SatCnr: number | null;
  ch02SatId: number | null;
  ch02SatCnr: number | null;
  ch03SatId: number | null;
  ch03SatCnr: number | null;
  ch04SatId: number | null;
  ch04SatCnr: number | null;
  ch05SatId: number | null;
  ch05SatCnr: number | null;
  ch06SatId: number | null;
  ch06SatCnr: number | null;
  ch07SatId: number | null;
  ch07SatCnr: number | null;
  ch08SatId: number | null;
  ch08SatCnr: number | null;
  ch09SatId: number | null;
  ch09SatCnr: number | null;
  ch10SatId: number | null;
  ch10SatCnr: number | null;
  ch11SatId: number | null;
  ch11SatCnr: number | null;
  ch12SatId: number | null;
  ch12SatCnr: number | null;
  idMortalityStatus: number;
  activity: number | null;
  mainVoltage: number;
  backupVoltage: number;
  temperature: number;
  transformedX: number | null;
  transformedY: number | null;
}

export type Task = {
  idcollar: number;
  collarkey: string;
};

/**
 * Class responsible for processing Vectronic GPS telemetry data and inserting it into the database.
 */
export class VectronicsService extends DBService {
  dbClient: PoolClient | undefined;
  queue: queueAsPromised<Task>;

  vectronicsApi: string;

  queueConcurrency: number;
  recordsPerInsert: number;

  /**
   * Creates an instance of VectronicsService.
   * @param connection - The database connection object.
   */
  constructor(connection: IDBConnection) {
    super(connection);

    this.vectronicsApi = process.env.VECTRONICS_URL || '';

    this.queueConcurrency = Number(process.env.VECTRONIC_QUEUE_CONCURRENCY) || 4;
    this.recordsPerInsert = Number(process.env.VECTRONIC_DB_INSERT_BATCH_SIZE) || 1000;

    this.queue = fastq.promise(this, this._queueWorker, this.queueConcurrency);
  }

  /**
   * Starts the process of fetching and processing telemetry data from the Vectronic API.
   *
   * @return {*}  {Promise<void>}
   * @memberof VectronicsService
   */
  async process(): Promise<void> {
    try {
      // Get a new DB client from the pool
      this.dbClient = await this.connection.getClient();

      // Get device credentials from the database
      const deviceCredentials = await this._getDeviceCredentials();

      // Queue the devices for processing
      for (const deviceCredential of deviceCredentials) {
        this.queue.push({ idcollar: deviceCredential.idcollar, collarkey: deviceCredential.collarkey });
      }

      // Wait for the queue to drain
      await this.queue.drain();
    } catch (error) {
      defaultLogger.error('Failed to process Vectronic telemetry data: ', error);
    } finally {
      // Release the DB client
      this.dbClient?.release();
    }
  }

  /**
   * Retrieves a list of device credentials from the database.
   *
   * @return {*}  {Promise<IDeviceCredential[]>}
   * @memberof VectronicsService
   */
  async _getDeviceCredentials(): Promise<IDeviceCredential[]> {
    if (!this.dbClient) {
      throw new Error('DB client is undefined');
    }

    const queryBuilder = getKnex().queryBuilder();

    queryBuilder.select('idcollar', 'collarkey').from('api_vectronic_credential');

    const { sql, bindings } = queryBuilder.toSQL().toNative();

    const response = await this.dbClient.query(sql, bindings as any[]);

    return response.rows;
  }

  /**
   * Processes a single task from the queue.
   *
   * @param {Task} task
   * @return {*}  {Promise<void>}
   * @memberof VectronicsService
   */
  async _queueWorker(task: Task): Promise<void> {
    try {
      const devices = await this._getDeviceTelemetry(task.idcollar, task.collarkey);

      return this._insertTelemetry(devices);
    } catch (error) {
      defaultLogger.error(`Failed to process device ${task.idcollar}:`, error);
    }
  }

  /**
   * Requests telemetry data for a single device from the Vectronic API.
   *
   * @param {number} idcollar
   * @param {string} collarkey
   * @return {*}  {Promise<IVectronicRecord[]>}
   * @memberof VectronicsService
   */
  async _getDeviceTelemetry(idcollar: number, collarkey: string): Promise<IVectronicRecord[]> {
    try {
      const response = await axios.get<IVectronicRecord[]>(`${this.vectronicsApi}/${idcollar}/gps`, {
        params: {
          collarkey: collarkey
        }
      });

      return response.data;
    } catch (error) {
      defaultLogger.error(`Failed to fetch data for device ${idcollar}:`, error);
    }

    return [];
  }

  /**
   * Inserts telemetry data records into the database.
   *
   * @param {IVectronicRecord[]} telemetryRecords
   * @return {*}  {Promise<void>}
   * @memberof VectronicsService
   */
  async _insertTelemetry(telemetryRecords: IVectronicRecord[]): Promise<void> {
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
          idposition: row.idPosition, // unique identifier for each record
          idcollar: row.idCollar,
          acquisitiontime: dayjs(row.acquisitionTime).toISOString(),
          scts: dayjs(row.scts).toISOString(),
          origincode: row.originCode,
          ecefx: row.ecefX,
          ecefy: row.ecefY,
          ecefz: row.ecefZ,
          latitude: row.latitude,
          longitude: row.longitude,
          height: row.height,
          dop: row.dop,
          idfixtype: row.idFixType,
          positionerror: row.positionError,
          satcount: row.satCount,
          ch01satid: row.ch01SatId,
          ch01satcnr: row.ch01SatCnr,
          ch02satid: row.ch02SatId,
          ch02satcnr: row.ch02SatCnr,
          ch03satid: row.ch03SatId,
          ch03satcnr: row.ch03SatCnr,
          ch04satid: row.ch04SatId,
          ch04satcnr: row.ch04SatCnr,
          ch05satid: row.ch05SatId,
          ch05satcnr: row.ch05SatCnr,
          ch06satid: row.ch06SatId,
          ch06satcnr: row.ch06SatCnr,
          ch07satid: row.ch07SatId,
          ch07satcnr: row.ch07SatCnr,
          ch08satid: row.ch08SatId,
          ch08satcnr: row.ch08SatCnr,
          ch09satid: row.ch09SatId,
          ch09satcnr: row.ch09SatCnr,
          ch10satid: row.ch10SatId,
          ch10satcnr: row.ch10SatCnr,
          ch11satid: row.ch11SatId,
          ch11satcnr: row.ch11SatCnr,
          ch12satid: row.ch12SatId,
          ch12satcnr: row.ch12SatCnr,
          idmortalitystatus: row.idMortalityStatus,
          activity: row.activity,
          mainvoltage: row.mainVoltage,
          backupvoltage: row.backupVoltage,
          temperature: row.temperature,
          transformedx: row.transformedX,
          transformedy: row.transformedY,
          geom: `st_setSrid(st_point(${row.longitude}, ${row.latitude}), 4326)`
        });
      });

      // @ts-ignore
      const queryBuilder = knex
        .queryBuilder()
        .insert(insertRows)
        .into('telemetry_api_lotek')
        .onConflict(knex.raw(`ON CONFLICT DO NOTHING RETURNING idPosition`))
        .ignore();

      //   const { sql, bindings } = queryBuilder.toSQL().toNative();

      //   const response = await this.dbClient.query(sql, bindings as any[]);

      //   insertedRowCount += response.rowCount;
      insertedRowCount += insertRows.length;
    }
    console.log(`Successfully inserted ${insertedRowCount} new Vectronics records`);
  }
}
