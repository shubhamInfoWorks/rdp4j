package io.infoworks.saas.ingestion.filewatchers;

import com.github.drapostolos.rdp4j.AbstractRdp4jListener;
import com.github.drapostolos.rdp4j.FileAddedEvent;
import com.github.drapostolos.rdp4j.FileModifiedEvent;
import com.github.drapostolos.rdp4j.FileRemovedEvent;

public class IngestionListener extends AbstractRdp4jListener {

  @Override
  public void fileAdded(FileAddedEvent event) throws InterruptedException {
    System.out.println("Added File" + event.getFileElement());
  }

  @Override
  public void fileRemoved(FileRemovedEvent event) throws InterruptedException {
    System.out.println("Removed File" + event.getFileElement());
  }

  @Override
  public void fileModified(FileModifiedEvent event) throws InterruptedException {
    System.out.println("Updated File" + event.getFileElement());
  }
}
