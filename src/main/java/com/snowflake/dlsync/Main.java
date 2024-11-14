package com.snowflake.dlsync;

import com.snowflake.dlsync.models.ChangeType;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class Main {

    public static void main(String[] args) throws SQLException {
        log.info("DlSync change Manager started.");
        ChangeManager changeManager = null;
        ChangeType changeType = null;
        boolean onlyHashes = false;
        List<String> schemas = null;
        try {
            changeType = args.length >= 1 ? ChangeType.valueOf(args[0]) : ChangeType.VERIFY;
            onlyHashes = args.length >= 2 ? args[1].equalsIgnoreCase("--only-hashes") : false;
            if( args.length >= 2 && args[1].equalsIgnoreCase("--schemas")) {
                schemas = Arrays.asList(Arrays.copyOfRange(args, 2, args.length));
            }
            changeManager = new ChangeManager();
            switch (changeType) {
                case DEPLOY:
                    changeManager.deploy(onlyHashes);
                    log.info("DLsync Changes deployed successfully.");
                    break;
                case ROLLBACK:
                    changeManager.rollback();
                    log.info("DLsync Changes rollback successfully.");
                    break;
                case VERIFY:
                    if(changeManager.verify()) {
                        log.info("DLsync Changes verified successfully.");
                    }
                    else {
                        log.error("DLsync Changes verification failed.");
                    }
                    break;
                case CREATE_SCRIPT:
                    changeManager.createAllScriptsFromDB(schemas);
                    log.info("DLsync created all scripts from DB.");
                    break;
                case CREATE_LINEAGE:
                    changeManager.createLineage();
                    log.info("DLsync successfully created lineage to DB.");
                    break;
                default:
                    log.error("Change type not specified as an argument.");
            }

        } catch (IOException e) {
            e.printStackTrace();
            log.error("Error: {} ", e);
            changeManager.endSyncError(changeType, e.getMessage());
            System.exit(2);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("Error: {} ", e);
            changeManager.endSyncError(changeType, e.getMessage());
            System.exit(3);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            log.error("Error: {} ", e);
            changeManager.endSyncError(changeType, e.getMessage());
            System.exit(4);
        }
        catch (Exception e) {
            e.printStackTrace();
            log.error("Error: {}", e.getMessage());
            changeManager.endSyncError(changeType, e.getMessage());
            System.exit(5);
        }
    }


}
