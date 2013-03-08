/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer.cli;

import com.ngdata.hbaseindexer.model.api.IndexDefinition;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * CLI tool for adding new indexes.
 */
public class AddIndexCli extends BaseIndexCli {
    private OptionSpec<String> nameOption;
    private OptionSpec<String> indexConfOption;

    public static void main(String[] args) throws Exception {
        new AddIndexCli().run(args);
    }

    public void run(OptionSet options) throws Exception {
        super.run(options);

        IndexDefinition indexDef = model.newIndex(nameOption.value(options));
        indexDef.setConfiguration(getIndexerConf(options, indexConfOption));
        model.addIndex(indexDef);

        System.out.println("Index added");
    }

    @Override
    protected OptionParser setupOptionParser() {
        OptionParser parser = super.setupOptionParser();

        nameOption = addNameOption(parser).required();
        indexConfOption = addIndexConfOption(parser).required();

        return parser;
    }

    @Override
    protected String getCmdName() {
        return "add-index";
    }
}
