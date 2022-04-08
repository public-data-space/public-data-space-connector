package de.fraunhofer.fokus.ids.models;

import de.fraunhofer.fokus.ids.messages.File;
import io.vertx.ext.web.FileUpload;

import java.util.Map;
import java.util.Set;

/**
 * @author Vincent Bohlen, vincent.bohlen@fokus.fraunhofer.de
 */
public class DataAssetDescription {

    private int sourceId;
    private Map data;
    private String datasourcetype;
    private Set<File> files;

    public Set<File> getFiles() {
        return files;
    }

    public void setFiles(Set<File> files) {
        this.files = files;
    }

    public int getSourceId() {
        return sourceId;
    }

    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }

    public Map getData() {
        return data;
    }

    public void setData(Map data) {
        this.data = data;
    }

    public String getDatasourcetype() {
        return datasourcetype;
    }

    public void setDatasourcetype(String datasourcetype) {
        this.datasourcetype = datasourcetype;
    }


}
