/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.management;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.cassandra.utils.AbstractGuavaIterator;
import picocli.CommandLine;

import static com.google.common.collect.ObjectArrays.concat;
import static org.apache.cassandra.management.CommandUtils.leadingSpaces;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_COMMAND_LIST_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_DESCRIPTION;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_DESCRIPTION_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_END_OF_OPTIONS;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_EXIT_CODE_LIST;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_EXIT_CODE_LIST_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_FOOTER;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_FOOTER_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_HEADER;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_HEADER_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_OPTION_LIST;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_OPTION_LIST_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_PARAMETER_LIST;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_PARAMETER_LIST_HEADING;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_SYNOPSIS;
import static picocli.CommandLine.Model.UsageMessageSpec.SECTION_KEY_SYNOPSIS_HEADING;

/**
 * Help facotry for the Cassandra nodetool to generate the help output. This class is used to match
 * the command output with the previously available nodetool help output format.
 */
public class CassandraHelpLayout extends CommandLine.Help
{
    public static final int DEFAULT_USAGE_HELP_WIDTH = 90;
    private static final String DESCRIPTION_HEADING = "NAME%n";
    private static final String SYNOPSIS_HEADING = "SYNOPSIS%n";
    private static final String OPTIONS_HEADING = "OPTIONS%n";
    private static final int COLUMN_INDENT = 8;
    private static final int DESCRIPTION_INDENT = 4;
    private static final CommandLine.Model.OptionSpec CASSANDRA_END_OF_OPTIONS_OPTION =
        CommandLine.Model.OptionSpec.builder("--")
                                    .description("This option can be used to separate command-line options from the " +
                                                 "list of argument, (useful when arguments might be mistaken for " +
                                                 "command-line options")
                                    .arity("0")
                                    .build();

    public CassandraHelpLayout(CommandLine.Model.CommandSpec spec, ColorScheme scheme)
    {
        super(spec, scheme);
    }

    @Override
    public String descriptionHeading(Object... params)
    {
        return createHeading(DESCRIPTION_HEADING, params);
    }

    /**
     * @param params Arguments referenced by the format specifiers in the header strings
     * @return the header string.
     */
    @Override
    public String description(Object... params) {
        CommandLine.Model.CommandSpec spec = commandSpec();
        String fullName = spec.qualifiedName();

        TextTable table = TextTable.forColumns(colorScheme(),
                                               new Column(spec.usageMessage().width() - COLUMN_INDENT, COLUMN_INDENT,
                                                          Column.Overflow.WRAP));
        table.setAdjustLineBreaksForWideCJKCharacters(spec.usageMessage().adjustLineBreaksForWideCJKCharacters());
        table.indentWrappedLines = 0;

        table.addRowValues(colorScheme().commandText(fullName)
                                        .concat(" - ")
                                        .concat(colorScheme().text(String.join(" ", spec.usageMessage().description()))));
        table.addRowValues(Ansi.OFF.new Text("", colorScheme()));
        return table.toString(new StringBuilder()).toString();
    }

    @Override
    public String synopsisHeading(Object... params)
    {
        return createHeading(SYNOPSIS_HEADING, params);
    }

    @Override
    public String detailedSynopsis(int synopsisHeadingLength, Comparator<CommandLine.Model.OptionSpec> optionSort, boolean clusterBooleanOptions)
    {
        Preconditions.checkState(synopsisHeadingLength >= 0,
                                 "synopsisHeadingLength must be a positive number but was " + synopsisHeadingLength);

        // Cassandra uses end of options delimiter in usage help.
        commandSpec().usageMessage().showEndOfOptionsDelimiterInUsageHelp(true);

        Set<CommandLine.Model.ArgSpec> argsInGroups = new HashSet<>();
        Ansi.Text groupsText = createDetailedSynopsisGroupsText(argsInGroups);

        List<Ansi.Text> optionsList = createCassandraSynopsisOptionsText(argsInGroups);
        Ansi.Text endOfOptionsText = createDetailedSynopsisEndOfOptionsText();
        Ansi.Text positionalParamText = createCassandraSynopsisPositionalsText(argsInGroups);
        Ansi.Text commandText = createDetailedSynopsisCommandText();

        CommandLine.Model.CommandSpec commandSpec = commandSpec();
        String parentCommandName = commandSpec.parent().qualifiedName();
        ColorScheme colorScheme = colorScheme();

        int leadingColumnWidth = parentCommandName.length() + COLUMN_INDENT;
        int followingColumnWidth = commandSpec.usageMessage().width() - leadingColumnWidth;
        TextTable textTable = TextTable.forColumns(colorScheme,
                                                   new Column(leadingColumnWidth, 0, Column.Overflow.TRUNCATE),
                                                   new Column(followingColumnWidth, 0, Column.Overflow.WRAP));
        textTable.setAdjustLineBreaksForWideCJKCharacters(commandSpec.usageMessage().adjustLineBreaksForWideCJKCharacters());
        textTable.indentWrappedLines = 0;

        Ansi.Text emptyCell = Ansi.OFF.new Text(leadingSpaces(leadingColumnWidth), colorScheme);
        Ansi.Text cmdPadding = Ansi.OFF.new Text(leadingSpaces(COLUMN_INDENT), colorScheme);
        Ansi.Text parentCommandText = cmdPadding.concat(colorScheme.commandText(parentCommandName)).concat(" ");
        // All other fields added to the synopsis are left-adjusted, so we don't need to align them.
        Ansi.Text text = groupsText.concat(" ")
                                   .concat(commandSpec.name())
                                   .concat(endOfOptionsText)
                                   .concat(" ")
                                   .concat(positionalParamText)
                                   .concat(commandText);

        LineBreakingOptionsIterator iter = new LineBreakingOptionsIterator(optionsList.iterator(), followingColumnWidth);
        boolean commandTextNotAdded = true;
        while (iter.hasNext())
        {
            Ansi.Text row = iter.next();
            Ansi.Text leadingCell = emptyCell;

            if (commandTextNotAdded)
            {
                leadingCell = parentCommandText;
                row = colorScheme.text(" ").concat(row);
                commandTextNotAdded = false;
            }

            if (iter.hasNext())
                textTable.addRowValues(leadingCell, row);
            else
                textTable.addRowValues(leadingCell, row.concat(text));
        }

        textTable.addRowValues(Ansi.OFF.new Text("", colorScheme));
        return textTable.toString();
    }

    private Ansi.Text createCassandraSynopsisPositionalsText(Collection<CommandLine.Model.ArgSpec> done)
    {
        List<CommandLine.Model.PositionalParamSpec> positionals = cassandraPositionals(commandSpec());
        positionals.removeAll(done);

        IParamLabelRenderer parameterLabelRenderer = createMinimalSpacedParamLabelRenderer();
        Ansi.Text text = colorScheme().text("");
        for (CommandLine.Model.PositionalParamSpec positionalParam : positionals)
        {
            Ansi.Text label = parameterLabelRenderer.renderParameterLabel(positionalParam, colorScheme().ansi(), colorScheme().parameterStyles());
            text = text.plainString().isEmpty() ? label : text.concat(" ").concat(label);
        }
        return text;
    }

    private List<Ansi.Text> createCassandraSynopsisOptionsText(Collection<CommandLine.Model.ArgSpec> done)
    {
        // Cassandra uses alphabetical order for options, ordered by short name.
        List<CommandLine.Model.OptionSpec> optionList = new ArrayList<>(commandSpec().options());
        optionList.sort(createShortOptionNameComparator());
        List<Ansi.Text> result = new ArrayList<>();
        optionList.removeAll(done);

        ColorScheme colorScheme = colorScheme();
        IParamLabelRenderer parameterLabelRenderer = createMinimalSpacedParamLabelRenderer();

        for (CommandLine.Model.OptionSpec option : optionList)
        {
            if (option.hidden())
                continue;

            Ansi.Text text = ansi().new Text(0);
            String nameString = option.shortestName();
            Ansi.Text name = colorScheme.optionText(nameString);
            Ansi.Text nameFull = colorScheme.optionText(option.longestName());
            text = text.concat("[(")
                       .concat(name)
                       .concat(spacedParamLabel(option, parameterLabelRenderer, colorScheme))
                       .concat(" | ")
                       .concat(nameFull)
                       .concat(spacedParamLabel(option, parameterLabelRenderer, colorScheme))
                       .concat(")]");

            result.add(text);
        }
        return result;
    }

    public static IParamLabelRenderer createMinimalSpacedParamLabelRenderer()
    {
        return new IParamLabelRenderer()
        {
            public Ansi.Text renderParameterLabel(CommandLine.Model.ArgSpec argSpec, Ansi ansi, List<Ansi.IStyle> styles)
            {
                ColorScheme colorScheme = CommandLine.Help.defaultColorScheme(ansi);
                if (argSpec.equals(CASSANDRA_END_OF_OPTIONS_OPTION))
                    return colorScheme.text("");
                if (argSpec instanceof CommandLine.Model.OptionSpec && argSpec.typeInfo().isBoolean())
                    return colorScheme.text("");
                return argSpec.isOption() ? colorScheme.optionText(argSpec.paramLabel()) :
                       colorScheme.parameterText(argSpec.paramLabel());
            }

            public String separator()
            {
                return "";
            }
        };
    }

    @Override
    public String optionListHeading(Object... params)
    {
        return createHeading(OPTIONS_HEADING, params);
    }

    @Override
    public String optionList()
    {
        Comparator<CommandLine.Model.OptionSpec> comparator = createShortOptionNameComparator();
        List<CommandLine.Model.OptionSpec> optionList = commandSpec().options();

        List<CommandLine.Model.OptionSpec> options = new ArrayList<>(optionList);
        options.sort(comparator);

        Layout layout = cassandraSingleColumnOptionsParametersLayout();
        layout.addAllOptions(options, createMinimalSpacedParamLabelRenderer());
        return layout.toString();
    }

    @Override
    public String endOfOptionsList() {
        Layout layout = cassandraSingleColumnOptionsParametersLayout();
        layout.addOption(CASSANDRA_END_OF_OPTIONS_OPTION, createMinimalSpacedParamLabelRenderer());
        return layout.toString();
    }

    private Layout cassandraSingleColumnOptionsParametersLayout()
    {
        TextTable table = TextTable.forColumns(colorScheme(), new Column(commandSpec().usageMessage().width() - COLUMN_INDENT,
                                                                         COLUMN_INDENT, Column.Overflow.WRAP));
        table.setAdjustLineBreaksForWideCJKCharacters(commandSpec().usageMessage().adjustLineBreaksForWideCJKCharacters());
        table.indentWrappedLines = DESCRIPTION_INDENT;
        return new Layout(colorScheme(), table, new CassandraStyleOptionRenderer(), new CassandraStyleParameterRenderer());
    }

    @Override
    public String parameterList()
    {
        List<CommandLine.Model.PositionalParamSpec> positionalParams = cassandraPositionals(commandSpec());
        Layout layout = cassandraSingleColumnOptionsParametersLayout();
        layout.addAllPositionalParameters(positionalParams, createMinimalSpacedParamLabelRenderer());
        return layout.toString();
    }

    @Override
    public String footerHeading(Object... params)
    {
        return createHeading("%n", params);
    }

    private static List<CommandLine.Model.PositionalParamSpec> cassandraPositionals(CommandLine.Model.CommandSpec commandSpec)
    {
        List<CommandLine.Model.PositionalParamSpec> positionals = new ArrayList<>(commandSpec.positionalParameters());
        for (CommandLine.Model.PositionalParamSpec param : positionals)
        {
            if (param.hidden())
            {
                if (param.description()[0].equals(CommandUtils.CASSANDRA_BACKWARD_COMPATIBLE_MARKER))
                {
                    positionals.clear();
                    positionals.add(param);
                    break;
                }
                else
                    positionals.remove(param);
            }
        }
        return positionals;
    }

    /**
     * Layout for cassandra help CLI output.
     * @return List of keys for the help sections.
     */
    public static List<String> cassandraHelpSectionKeys()
    {
        List<String> result = new LinkedList<>();
        result.add(SECTION_KEY_HEADER_HEADING);
        result.add(SECTION_KEY_HEADER);
        result.add(SECTION_KEY_DESCRIPTION_HEADING);
        result.add(SECTION_KEY_DESCRIPTION);
        result.add(SECTION_KEY_SYNOPSIS_HEADING);
        result.add(SECTION_KEY_SYNOPSIS);
        result.add(SECTION_KEY_OPTION_LIST_HEADING);
        result.add(SECTION_KEY_OPTION_LIST);
        result.add(SECTION_KEY_END_OF_OPTIONS);
        result.add(SECTION_KEY_PARAMETER_LIST_HEADING);
        result.add(SECTION_KEY_PARAMETER_LIST);
        result.add(SECTION_KEY_COMMAND_LIST_HEADING);
        result.add(SECTION_KEY_COMMAND_LIST);
        result.add(SECTION_KEY_EXIT_CODE_LIST_HEADING);
        result.add(SECTION_KEY_EXIT_CODE_LIST);
        result.add(SECTION_KEY_FOOTER_HEADING);
        result.add(SECTION_KEY_FOOTER);
        return result;
    }

    private static class LineBreakingOptionsIterator extends AbstractGuavaIterator<Ansi.Text>
    {
        private final Iterator<Ansi.Text> optionsIterator;
        private final int width;
        private Ansi.Text prev;

        LineBreakingOptionsIterator(Iterator<Ansi.Text> optionsIterator, int width)
        {
            this.optionsIterator = optionsIterator;
            this.width = width;
        }

        @Override
        protected Ansi.Text computeNext()
        {
            while (optionsIterator.hasNext())
            {
                Ansi.Text next = optionsIterator.next();
                if (prev == null)
                    prev = next;

                Ansi.Text curr;
                if (prev == next)
                    curr = next;
                else
                    curr = prev.concat(" ").concat(next);

                if (curr.plainString().length() > width)
                {
                    Ansi.Text result = prev;
                    prev = next;
                    return result;
                }
                else
                {
                    prev = curr;
                }
            }

            if (prev == null)
                return endOfData();
            else
            {
                Ansi.Text result = prev;
                prev = null;
                return result;
            }
        }
    }

    private static Ansi.Text spacedParamLabel(CommandLine.Model.OptionSpec optionSpec,
                                       IParamLabelRenderer parameterLabelRenderer,
                                       ColorScheme scheme)
    {
        return optionSpec.typeInfo().isBoolean() ? scheme.text("") :
               scheme.text(" ").concat(parameterLabelRenderer.renderParameterLabel(optionSpec, scheme.ansi(), scheme.optionParamStyles()));
    }

    private static class CassandraStyleOptionRenderer implements IOptionRenderer
    {
        public Ansi.Text[][] render(CommandLine.Model.OptionSpec option, IParamLabelRenderer parameterLabelRenderer, ColorScheme scheme)
        {
            Ansi.Text optionText = scheme.optionText("");
            for (int i = 0; i < option.names().length; i++)
            {
                String name = option.names()[i];
                optionText = optionText.concat(scheme.optionText(name))
                                       .concat(spacedParamLabel(option, parameterLabelRenderer, scheme))
                                       .concat(i == option.names().length - 1 ? "" : ", ");
            }

            Ansi.Text descPadding = Ansi.OFF.new Text(leadingSpaces(DESCRIPTION_INDENT), scheme);
            Ansi.Text desc = scheme.optionText(option.description().length == 0 ? "" : option.description()[0]);

            Ansi.Text[][] result = new Ansi.Text[3][];
            result[0] = new Ansi.Text[]{optionText};
            result[1] = new Ansi.Text[]{descPadding.concat(desc)};
            result[2] = new Ansi.Text[]{Ansi.OFF.new Text("", scheme)};
            return result;
        }
    }

    private static class CassandraStyleParameterRenderer implements IParameterRenderer
    {
        @Override
        public Ansi.Text[][] render(CommandLine.Model.PositionalParamSpec param, IParamLabelRenderer parameterLabelRenderer, ColorScheme scheme)
        {
            String descriptionString = param.description()[0].equals(CommandUtils.CASSANDRA_BACKWARD_COMPATIBLE_MARKER) ?
                                       param.description()[1] : param.description()[0];

            Ansi.Text descPadding = Ansi.OFF.new Text(leadingSpaces(DESCRIPTION_INDENT), scheme);
            Ansi.Text[][] result = new Ansi.Text[3][];
            result[0] = new Ansi.Text[]{ parameterLabelRenderer.renderParameterLabel(param, scheme.ansi(), scheme.parameterStyles()) };
            result[1] = new Ansi.Text[]{ descPadding.concat(scheme.text(descriptionString)) };
            result[2] = new Ansi.Text[]{ Ansi.OFF.new Text("", scheme) };
            return result;
        }
    }
}
