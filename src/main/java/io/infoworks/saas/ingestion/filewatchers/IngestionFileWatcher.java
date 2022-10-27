package io.infoworks.saas.ingestion.filewatchers;

import com.github.drapostolos.rdp4j.DirectoryPoller;
import com.github.drapostolos.rdp4j.spi.PolledDirectory;
import io.infoworks.saas.ingestion.filewatchers.cloud.CloudDirectory;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;

public class IngestionFileWatcher {

  public static void main(String[] args) throws InterruptedException {
//    PolledDirectory polledDirectory = new SFTPDirectory("10.17.3.198",
//        "/SAIKRISHNA/csv_files/airtravels/temp_files/", "testing", "IN11**rk");
    Configuration configuration =
        getWasbConfiguration(
            "ingestioncsv",
            "iwstorage1",
            "sKIDF5Qn3Qe8Jt4myCPe1+ZORiEQkFISgGgWB1Dz9I45oKt/h6ffWYRJBSM+MQJfr41uWcugYlVv8xiq0AiVkQ==");
    PolledDirectory polledDirectory = new CloudDirectory(configuration,
        "wasbs://ingestioncsv@iwstorage1.blob.core.windows.net/csv_files/airtravels/");
    DirectoryPoller dp = DirectoryPoller.newBuilder()
        .addPolledDirectory(polledDirectory)
        .addListener(new IngestionListener())
        .enableFileAddedEventsForInitialContent()
        .setPollingInterval(10, TimeUnit.SECONDS)
        .start();

    TimeUnit.HOURS.sleep(2);

    dp.stop();
  }

  public static Configuration getWasbConfiguration(
      String containerName,
      String accountName,
      String accountKey) {
    Configuration configuration = new Configuration();
    String wasbExtension = "blob.core.windows.net";
    configuration.set("fs.azure.account.key." + accountName + "." + wasbExtension, accountKey);
    return configuration;
  }
}

