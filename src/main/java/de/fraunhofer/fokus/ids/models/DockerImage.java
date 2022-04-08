package de.fraunhofer.fokus.ids.models;

import java.util.List;
import java.util.UUID;


public class DockerImage {
	private String imageId;
    private String name;
    private List<String> containerIds;
    private String uuid;

    public List<String> getContainerIds() {
        return containerIds;
    }

    public void setContainerIds(List<String> containerIds) {
        this.containerIds = containerIds;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getId() {
        return imageId;
    }

    public void setId(String id) {
        this.imageId = id;
    }

    public void setUUID(String uuid) { this.uuid = uuid; }

    public String getUUID() { return uuid; }

}
