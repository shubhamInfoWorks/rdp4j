package io.infoworks.saas.ingestion.filewatchers.cloud;

import com.github.drapostolos.rdp4j.spi.FileElement;
import com.github.drapostolos.rdp4j.spi.PolledDirectory;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CloudDirectory implements PolledDirectory {

  Configuration configuration;
  String workingDirectory;

  public CloudDirectory(Configuration configuration, String workingDirectory) {
    this.configuration = configuration;
    this.workingDirectory = workingDirectory;
  }

  public Set<FileElement> listFiles() throws IOException {
    HashSet<FileElement> files = new HashSet();
    try {
      Path path = new Path(workingDirectory);
      FileSystem fileSystem = path.getFileSystem(configuration);
      FileStatus[] fileStatuses = fileSystem.listStatus(path);
      for (FileStatus fileStatus : fileStatuses) {
        files.add(new CloudFile(fileStatus));
      }
    } catch (Exception e) {
      System.out.println(ExceptionUtils.getStackTrace(e));
      throw new IOException(e);
    }
    System.out.println("size" + files.size());
    return files;
  }
}
