package de.fraunhofer.fokus.ids.persistence.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import de.fraunhofer.fokus.ids.persistence.enums.DataAssetStatus;

import java.util.Map;
import java.util.Set;

public class Dataset extends Resource {
	
	//Resource extends BaseEntity: String title, String description, String publisher, String license, String resourceId
	//BaseEntity: Long id, Instant createdAt, Instant updatedAt
	
    private Set<Distribution> distributions;
	private DataAssetStatus status;
    private Set<String> tags;
    private String version;
    @JsonProperty("sourceid")
    private Long sourceId;
    
    private String pid;
    private String author;
    private String data_access_level;
    private Map<String, Set<String>> additionalmetadata;

    public Set<Distribution> getDistributions() {
        return distributions;
    }

    public void setDistributions(Set<Distribution> distributions) {
        this.distributions = distributions;
    }

    public DataAssetStatus getStatus() {
        return status;
    }

    public void setStatus(DataAssetStatus status) {
        this.status = status;
    }

    public Set<String> getTags() {
        return tags;
    }

    public void setTags(Set<String> tags) {
        this.tags = tags;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Long getSourceId() {
        return sourceId;
    }

    public void setSourceId(Long sourceId) {
        this.sourceId = sourceId;
    }
    
    public String getPid() {
		return pid;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public String getData_access_level() {
		return data_access_level;
	}

	public void setData_access_level(String data_access_level) {
		this.data_access_level = data_access_level;
	}

	public Map<String, Set<String>> getAdditionalmetadata() {
		return additionalmetadata;
	}

	public void setAdditionalmetadata(Map<String, Set<String>> additionalmetadata) {
		this.additionalmetadata = additionalmetadata;
	}

}
