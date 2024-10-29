package com.gotocompany.depot.maxcompute.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
public class RecordWrappers {
    private List<RecordWrapper> validRecords;
    private List<RecordWrapper> invalidRecords;

    public RecordWrappers() {
        this.validRecords = new ArrayList<>();
        this.invalidRecords = new ArrayList<>();
    }

    public void addValidRecord(RecordWrapper recordWrapper) {
        this.validRecords.add(recordWrapper);
    }

    public void addInvalidRecord(RecordWrapper recordWrapper) {
        this.invalidRecords.add(recordWrapper);
    }

}
