import { idir, request } from '../utils/constants';

const req = request.post('/import-xml').query(idir);

describe('KeyX Import Endpoint', () => {
  describe('POST /import-xml', () => {
    it('should be reachable and return 200 status', async () => {
      const res = await req;
      expect(res.status).toBe(200);
    });
    describe('given valid keyX file that already exists in DB', () => {
      it('should return single error message', async () => {
        const res = await request
          .post('/import-xml')
          .query(idir)
          .attach('xml', 'src/tests/utils/files/Collar45323_Registration.keyx');
        expect(res.body.errors.length).toBe(1);
      });
    });
    describe('given valid keyX file that does not exist in DB', () => {
      it('should insert keyX into DB and return insertion results', async () => {
        const res = await request
          .post('/import-xml')
          .query(idir)
          .attach('xml', 'src/tests/utils/files/Collar45333_Registration.keyx');
        expect(res.body.results.length).toBe(1);
      });
    });
    describe('given empty keyX file', () => {
      it('should return single error', async () => {
        const res = await req.send({});
        expect(res.body.errors.length).toBe(1);
        expect(res.body.errors[0].error).toBe('no attached files found');
        expect(res.body.errors[0].rownum).toBe(-1);
      });
    });
  });
});
