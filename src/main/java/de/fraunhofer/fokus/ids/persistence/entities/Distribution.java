package de.fraunhofer.fokus.ids.persistence.entities;

import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Distribution extends Resource {
	
	//Resource extends BaseEntity: String title, String description, String publisher, String license, String resourceId
	//BaseEntity: Long id, Instant createdAt, Instant updatedAt

    private String filename;
    private String filetype;
    @JsonProperty("datasetid")
    private String datasetId;
    
    private int byte_size;
    private Map<String, Set<String>> additionalmetadata;

    public int getByte_size() {
		return byte_size;
	}

	public void setByte_size(int byte_size) {
		this.byte_size = byte_size;
	}

	public Map<String, Set<String>> getAdditionalmetadata() {
		return additionalmetadata;
	}

	public void setAdditionalmetadata(Map<String, Set<String>> additionalmetadata) {
		this.additionalmetadata = additionalmetadata;
	}

	public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getFiletype() {
        return filetype;
    }

    public void setFiletype(String filetype) {
        this.filetype = filetype;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

}
