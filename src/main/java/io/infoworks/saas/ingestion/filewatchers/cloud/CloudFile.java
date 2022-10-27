package io.infoworks.saas.ingestion.filewatchers.cloud;

import com.github.drapostolos.rdp4j.spi.FileElement;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;

public class CloudFile implements FileElement {

  private final FileStatus fileStatus;

  public CloudFile(FileStatus fileStatus) {
    this.fileStatus = fileStatus;
  }

  @Override
  public long lastModified() throws IOException {
    return fileStatus.getModificationTime();
  }

  @Override
  public boolean isDirectory() {
    return fileStatus.isDirectory();
  }

  @Override
  public String getName() {
    return fileStatus.getPath().getName();
  }

  @Override
  public String toString() {
    return fileStatus.getPath().getName();
  }
}
