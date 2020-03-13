import { IDestinationsListStore } from '@stores/destinationsList';
import { ISourcesListStore } from '@stores/sourcesList';
import { Table } from 'antd';
import { inject, observer } from 'mobx-react';
import React, { Component } from 'react';
import { RouteComponentProps, withRouter } from 'react-router-dom';
import SourceIcon from '@components/icons/sourceIcon';
import DestinationIcon from '../icons/destinationIcon';

import {
  Container,
  HeadingContainer,
  HeadingTag,
  IconSpacing,
  MainHeading,
  TableViewContainer,
} from './styles';
import theme from '@css/theme';

interface IConfiguredDestinationsProps extends RouteComponentProps<any> {
  history: any;
  sourcesListStore?: ISourcesListStore;
  destinationsListStore?: IDestinationsListStore;
}

interface IConfiguredDestinationsState {
  count: number;
  active: number;
  inactive: number;
}
@inject('sourcesListStore', 'destinationsListStore')
@observer
class ConfiguredDestinations extends Component<
  IConfiguredDestinationsProps,
  IConfiguredDestinationsState
> {
  constructor(props: IConfiguredDestinationsProps) {
    super(props);
    this.state = {
      count: 1,
      active: 1,
      inactive: 0,
    };
  }

  public async componentDidMount() {
    const { sourcesListStore, destinationsListStore } = this.props;
    await this.constructDataSource(
      sourcesListStore!.sources,
      destinationsListStore!.destinations,
    );
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
      title: 'Sources',
      dataIndex: 'sources',
      key: 'sources',
    },
  ];

  public constructDataSource = (sources: any, destinations: any) => {
    this.dataSource = [];
    let activeCount = 0;

    for (let i = 0; i < destinations.length; i++) {
      const sourceList: any = [];
      const obj: any = {};

      obj.key = destinations[i].id;
      for (let scount = 0; scount < destinations[i].sources.length; scount++) {
        const a = (
          <IconSpacing key={`${destinations[i].sources[scount].id}`}>
            <SourceIcon
              key={`${scount}`}
              source={destinations[i].sources[scount].sourceDef.name}
            />
          </IconSpacing>
        );
        sourceList.push(a);
      }

      if (destinations[i].enabled) {
        activeCount++;
      }
      obj.sources = sourceList;
      obj.icon = (
        <DestinationIcon
          destination={destinations[i].destinationDefinition.name}
        />
      );
      obj.id = destinations[i].id;
      obj.name = destinations[i].name;
      obj.status = destinations[i].enabled ? 'Enabled' : 'Disabled';
      this.dataSource.push(obj);
    }
    this.setState({ active: activeCount });
    this.setState({ count: sources.length });
    this.setState({ inactive: this.state.count - this.state.active });
  };

  handleRowClick = (record: any, Index: Number) => {
    const { history } = this.props;
    history!.push(`/destinations/${record.id}`);
  };

  public render() {
    const { count, active, inactive } = this.state;
    return (
      <Container className="Sources">
        <HeadingContainer>
          <MainHeading>Configured Destinations</MainHeading>
          <HeadingTag>
            {count} Added {active} Active {inactive} Inactive
          </HeadingTag>
        </HeadingContainer>
        <TableViewContainer>
          <Table
            dataSource={this.dataSource}
            columns={this.columns}
            pagination={false}
            onRow={(record, Index) => ({
              onClick: () => this.handleRowClick(record, Index),
            })}
          />
        </TableViewContainer>
      </Container>
    );
  }
}

export default withRouter<IConfiguredDestinationsProps, any>(
  ConfiguredDestinations,
);
