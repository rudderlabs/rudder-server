import { Flex } from '@components/common/misc';
import SourceIcon from '@components/icons/sourceIcon';
import { IDestinationsListStore } from '@stores/destinationsList';
import { ISourcesListStore } from '@stores/sourcesList';
import { Table } from 'antd';
import { inject, observer } from 'mobx-react';
import React, { Component } from 'react';
import { Link, RouteComponentProps, withRouter } from 'react-router-dom';
import DestinationIcon from '../icons/destinationIcon';

import {
  AddSource,
  Container,
  HeadingTag,
  MainHeading,
  TableViewContainer,
  IconStyle,
  IconSpacing,
} from './styles';
import Svg from '@svg/index';
import theme from '@css/theme';

interface IConfiguredSourcesProps extends RouteComponentProps<any> {
  history: any;
  sourcesListStore?: ISourcesListStore;
  destinationsListStore?: IDestinationsListStore;
}

interface IConfiguredSourcesState {
  count: number;
  active: number;
  inactive: number;
}
@inject('sourcesListStore', 'destinationsListStore')
@observer
class ConfiguredSources extends Component<
  IConfiguredSourcesProps,
  IConfiguredSourcesState
> {
  constructor(props: IConfiguredSourcesProps) {
    super(props);
    this.state = {
      count: 1,
      active: 1,
      inactive: 0,
    };
  }
  dataSource: any = [];
  columns = [
    {
      title: '',
      dataIndex: 'icon',
      key: 'icon',
    },
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
    },
    {
      title: 'Destination',
      dataIndex: 'destination',
      key: 'destination',
    },
  ];

  public async componentDidMount() {
    const { sourcesListStore } = this.props;
    await this.constructDataSource(sourcesListStore!.sources);
  }

  public constructDataSource = (sources: any) => {
    this.dataSource = [];
    let activeCount = 0;
    for (let i = 0; i < sources.length; i++) {
      const destinationList: any = [];
      const obj: any = {};

      obj.key = i;
      for (let dcount = 0; dcount < sources[i].destinations.length; dcount++) {
        const a = (
          <IconSpacing key={`${sources[i].destinations[dcount].id}`}>
            <DestinationIcon
              destination={
                sources[i].destinations[dcount].destinationDefinition.name
              }
            />
          </IconSpacing>
        );
        destinationList.push(a);
      }
      if (sources[i].enabled) {
        activeCount++;
      }
      obj.destination = destinationList;
      obj.icon = <SourceIcon source={sources[i].sourceDef.name} />;
      obj.id = sources[i].id;
      obj.name = sources[i].name;
      obj.status = sources[i].enabled ? 'Enabled' : 'Disabled';
      this.dataSource.push(obj);
    }
    this.setState({ count: sources.length });
    this.setState({ active: activeCount });
    this.setState({ inactive: this.state.count - this.state.active });
  };

  handleRowClick = (record: any, Index: Number) => {
    const { history } = this.props;
    history!.push(`/sources/${record.id}`);
  };

  public renderAddSource = () => {
    return (
      <Link to="/sources/setup">
        <Flex flexDirection={'row'}>
          <IconStyle>
            <Svg name="plus" />
          </IconStyle>
          <AddSource>Add source</AddSource>
        </Flex>
      </Link>
    );
  };

  public render() {
    const { count, active, inactive } = this.state;
    return (
      <Container>
        <Flex flexDirection="column">
          <MainHeading>Configured Sources</MainHeading>
          <HeadingTag className={'p-v-xs'}>
            {count} Added {active} Active {inactive} Inactive
          </HeadingTag>
        </Flex>
        <TableViewContainer>
          <Table
            dataSource={this.dataSource}
            columns={this.columns}
            pagination={false}
            footer={this.renderAddSource}
            onRow={(record, Index) => ({
              onClick: () => this.handleRowClick(record, Index),
            })}
          />
        </TableViewContainer>
      </Container>
    );
  }
}

export default withRouter<IConfiguredSourcesProps, any>(ConfiguredSources);
